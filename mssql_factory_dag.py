from datetime import datetime, timedelta
from pathlib import Path
import yaml

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor

from custom_plugins.tg_notifications import send_telegram_sla_alert, send_telegram_alert
from custom_plugins.mssql_utils import call_procedure

mssql_connection_id_str = "some_connection"
projects_path = Path(Variable.get("airflow_projects_path"))
yaml_dags_folder_name = "mssql_factory"
default_function_table_checker_name = "some_checker_functions"

def get_sql_query_table_checker(function_name: str, table_name: str, execution_date: str):
    return f"SELECT * FROM {function_name}('{table_name}', '{execution_date}')"

def create_conditional_check(condition_str: str, group_id: str):
    """
    Create a conditional check function that evaluates the given condition string.
    
    Args:
        condition_str: Python expression to evaluate (e.g., "execution_date.day == 1")
        group_id: Task group ID to return if condition is True
    
    Returns:
        Callable function for BranchPythonOperator
    """
    def check_condition(**context):
        execution_date = context["data_interval_end"]
        
        safe_namespace = {
            "execution_date": execution_date,
            "datetime": datetime,
        }
        
        try:
            result = eval(condition_str, {"__builtins__": {}}, safe_namespace)
            
            if result:
                return f"{group_id}_tasks"
            return f"skip_{group_id}_task"
        except Exception as e:
            print(f"Error evaluating condition '{condition_str}': {e}")
            return f"skip_{group_id}_task"
    
    return check_condition

# checks upstream task states and fails the dag if any task failed; used for 'continue_on_failure' mode.
def finalize_dag(**kwargs):
    ti = kwargs['ti']
    dag_run = ti.get_dagrun()
    task_instances = dag_run.get_task_instances()
    
    # get all task states except start, end, skip, and check tasks
    failed_tasks = []
    for task_instance in task_instances:
        task_id = task_instance.task_id
        if task_id in ['start', 'end'] or task_id.startswith('skip_') or task_id.startswith('check_') or task_id.startswith('join_'):
            continue
        
        if task_instance.state == 'failed':
            failed_tasks.append(task_id)
    
    if failed_tasks:
        error_msg = f"One or more tasks failed: {', '.join(failed_tasks)}. Marking the DAG as failed."
        print(error_msg)
        raise ValueError(error_msg)
    
    print("All tasks completed successfully or were skipped.")

def create_dag_from_config(config_path: Path) -> DAG:
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    dag_id = config["dag_id"]
    
    alerts_cfg = config.get("alerts", {})
    on_failure_cb = send_telegram_alert if alerts_cfg.get("on_failure", False) else None
    sla_cb = send_telegram_sla_alert if alerts_cfg.get("on_sla_miss", False) else None

    # execution mode: 'stop_on_failure' (default) or 'continue_on_failure'
    execution_mode = config.get("execution_mode", "stop_on_failure")
    
    default_args = {
        "owner": "airflow",
        "retries": config.get("retries", 0), # 1
        "catchup": config.get("catchup", False),
        "retry_delay": timedelta(minutes=config.get("retry_delay_minutes", 0)), # 2
        "on_failure_callback": send_telegram_alert,
    }
    
    if on_failure_cb:
        default_args["on_failure_callback"] = on_failure_cb

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=config["schedule"],
        start_date=datetime.strptime(config["start_date"], "%Y-%m-%d"),
        max_active_runs=config.get("max_active_runs", 1),
        max_active_tasks=config.get("max_active_tasks", 1),
        tags=[config["version"]] + config.get("tags", []),
        sla_miss_callback=sla_cb,
    )
    
    with dag:
        step_start = EmptyOperator(task_id="start")
        
        if execution_mode == "continue_on_failure":
            step_end = PythonOperator(
                task_id="end",
                python_callable=finalize_dag,
                trigger_rule=TriggerRule.ALL_DONE,
                retries=0, # don't retry the finalize task
            )
        else: # stop_on_failure
            step_end = EmptyOperator(
                task_id="end", 
                trigger_rule=TriggerRule.NONE_FAILED
            )
        
        prev_task = step_start
        
        default_trigger_rule = TriggerRule.ALL_DONE if execution_mode == "continue_on_failure" else TriggerRule.ALL_SUCCESS
        all_sensors = []
        
        # dag sensor
        if "wait_for" in config:
            wait_configs = config["wait_for"]
            # support both single dict and list of dicts
            if not isinstance(wait_configs, list):
                wait_configs = [wait_configs]
            
            for wait_config in wait_configs:
                wait_task = ExternalTaskSensor(
                    task_id=f"wait_for_dag_{wait_config['external_dag_id']}",
                    external_dag_id=wait_config["external_dag_id"],
                    external_task_id=wait_config["external_task_id"],
                    execution_delta=timedelta(
                        hours=wait_config.get("execution_delta_hours", 0),
                        minutes=wait_config.get("execution_delta_minutes", 0)
                    ),
                    allowed_states=["success"],
                    failed_states=["failed", "skipped"],
                    mode="poke",
                    poke_interval=60,
                    timeout=wait_config.get("timeout", 600)
                )
                all_sensors.append(wait_task)
        
        # table sensors (wait_for_tables)
        if "wait_for_tables" in config:
            table_configs = config["wait_for_tables"]
            # support both single dict and list of dicts
            if not isinstance(table_configs, list):
                table_configs = [table_configs]
            
            for idx, table_config in enumerate(table_configs):
                table_name = table_config["table_name"]
                execution_date = table_config.get("execution_date", '{{data_interval_end.strftime("%Y-%m-%d")}}')
                function_name = table_config.get("function", default_function_table_checker_name)

                sql_query = get_sql_query_table_checker(function_name, table_name, execution_date)
                task_id = f"wait_for_table_{table_name}"
                
                table_sensor = SqlSensor(
                    task_id=task_id,
                    conn_id=mssql_connection_id_str,
                    sql=sql_query,
                    mode="poke",
                    poke_interval=table_config.get("poke_interval", 60),
                    timeout=table_config.get("timeout", 600),
                )
                all_sensors.append(table_sensor)

        # connect all sensors sequential
        if all_sensors:
            for sensor in all_sensors:
                prev_task >> sensor
                prev_task = sensor
            
            # if sensors exist, override trigger rule for procedures
            # procedures must not run if sensors fail (even in continue_on_failure mode)
            if execution_mode == "continue_on_failure":
                default_trigger_rule = TriggerRule.NONE_FAILED

        if "procedures" in config:
            for item in config["procedures"]:
                
                # task group (with or without condition)
                if "task_group" in item:
                    group_name = item["task_group"]
                    group_procedures = item.get("procedures", [])
                    condition = item.get("condition")
                    
                    if not group_procedures:
                        continue
                    
                    with TaskGroup(group_id=f"{group_name}_tasks") as task_group:
                        prev_in_group = None
                        for proc_config in group_procedures:
                            procedure_full_name = f"{proc_config['database']}.{proc_config['schema']}.{proc_config['name']}"
                            
                            trigger_rule = proc_config.get("trigger_rule", default_trigger_rule)
                            
                            task = PythonOperator(
                                task_id=f"call_{proc_config['name']}_task",
                                python_callable=call_procedure,
                                op_kwargs={
                                    "procedure_name": procedure_full_name,
                                    "params": proc_config.get("params", {})
                                },
                                trigger_rule=trigger_rule,
                            )
                            
                            if prev_in_group:
                                prev_in_group >> task
                            prev_in_group = task
                    
                    if condition:
                        check_task = BranchPythonOperator(
                            task_id=f"check_{group_name}_condition",
                            python_callable=create_conditional_check(condition, group_name),
                            trigger_rule=TriggerRule.NONE_FAILED,
                        )
                        
                        skip_task = EmptyOperator(
                            task_id=f"skip_{group_name}_task",
                            trigger_rule=TriggerRule.NONE_FAILED,
                        )
                        
                        # connect: prev_task -> check -> [skip, task_group]
                        prev_task >> check_task
                        check_task >> [skip_task, task_group]
                        
                        # both branches continue to next task
                        join_task = EmptyOperator(
                            task_id=f"join_after_{group_name}",
                            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                        )
                        
                        skip_task >> join_task
                        task_group >> join_task
                        
                        prev_task = join_task
                    else:
                        # no condition - simple connection
                        prev_task >> task_group
                        prev_task = task_group
                
                # regular procedure (always runs, no condition support)
                else:
                    procedure_full_name = f"{item['database']}.{item['schema']}.{item['name']}"
                    trigger_rule = item.get("trigger_rule", default_trigger_rule)
                    
                    task = PythonOperator(
                        task_id=f"call_{item['name']}_task",
                        python_callable=call_procedure,
                        op_kwargs={
                            "procedure_name": procedure_full_name,
                            "params": item.get("params", {})
                        },
                        trigger_rule=trigger_rule,
                    )
                    
                    prev_task >> task
                    prev_task = task
        
        if prev_task and prev_task != step_start:
            prev_task >> step_end
        else:
            step_start >> step_end
    
    return dag

# find all yaml files in subdirectories
for config_file in projects_path.glob(f"{yaml_dags_folder_name}/*.yaml"):
    try:
        dag = create_dag_from_config(config_file)
        # register dag in globals so airflow can find it
        globals()[dag.dag_id] = dag
        print(f"successfully loaded DAG: {dag.dag_id}")
    except Exception as e:
        print(f"error loading DAG from {config_file}: {e}")
        import traceback
        traceback.print_exc()