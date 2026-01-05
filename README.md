# Airflow MSSQL Factory

Dynamic Airflow DAG generator for MSSQL stored procedures. Define your DAGs in YAML configuration files with support for conditional task groups, external DAG sensors, table sensors, and flexible execution modes.

## Features

- **YAML-based configuration** - Define DAGs declaratively without writing Python code
- **Conditional task groups** - Execute procedures based on conditions (day of month, day of week, etc.)
- **External dependencies** - Wait for other DAGs or database tables before execution
- **Flexible execution modes** - Choose between `stop_on_failure` or `continue_on_failure`
- **Telegram alerts** - Built-in support for failure and SLA miss notifications
- **Task groups** - Organize related procedures together
- **Customizable** - Configure retries, timeouts, schedules, and more per DAG

## Installation

1. Clone this repository to your Airflow project:

```bash
git clone https://github.com/yourusername/airflow-mssql-factory.git
cd airflow-mssql-factory
```

2. Copy `mssql_factory_dag.py` to your Airflow DAGs folder

3. Create a directory for YAML configs (default: `mssql_factory/` in your Airflow projects path)

4. Set the Airflow variable `airflow_projects_path` to point to your configs directory:

```bash
airflow variables set airflow_projects_path "/path/to/your/configs"
```

5. Project has custom plugins:
   - `custom_plugins.tg_notifications` - for Telegram alerts
   - `custom_plugins.mssql_utils` - for calling MSSQL procedures

6. Configure your MSSQL connection in Airflow

## Quick Start

Create a YAML file in `mssql_factory/` directory (e.g., `my_etl_dag.yaml`):

```yaml
dag_id: "my_etl_pipeline"
version: "v1.0.0"
schedule: "0 9 * * *"
start_date: "2026-01-03"
tags: ["etl"]

alerts:
  on_failure: false
  on_sla_miss: false

procedures:
  - database: "DATABASE"
    schema: "etl"
    name: "load_customers"
  
  - database: "DATABASE"
    schema: "etl"
    name: "load_orders"
```

The DAG will be automatically discovered and loaded by Airflow!

## Configuration Reference

### Basic DAG Settings

```yaml
dag_id: "unique_dag_name"          # Required: Unique DAG identifier
version: "v1.0.0"                  # Required: Version tag
schedule: "0 9 * * *"              # Required: Cron schedule
start_date: "2026-01-03"           # Required: First execution date
tags: ["etl"]                      # Optional: DAG tags
catchup: false                     # Optional: Run historical DAG runs (default: false)
max_active_runs: 1                 # Optional: Max concurrent runs (default: 1)
max_active_tasks: 1                # Optional: Max concurrent tasks (default: 1)
retries: 1                         # Optional: Task retry count (default: 0)
retry_delay_minutes: 2             # Optional: Delay between retries (default: 0)
```

### Execution Modes

```yaml
execution_mode: "stop_on_failure"      # Stop DAG immediately if any task fails (default)
# OR
execution_mode: "continue_on_failure"  # Continue running other tasks, fail at the end
```

### Alerts

```yaml
alerts:
  on_failure: false      # Send Telegram alert on task failure
  on_sla_miss: false     # Send Telegram alert on SLA miss
```

### External DAG Dependencies

Wait for other DAGs to complete before starting:

```yaml
wait_for:
  - external_dag_id: "upstream_dag"
    external_task_id: "end"
    execution_delta_hours: 1
    execution_delta_minutes: 30
    timeout: 600
  
  - external_dag_id: "another_dag"
    external_task_id: "end"
    timeout: 300
```

### Table Dependencies

Wait for tables to be updated before starting:

```yaml
wait_for_tables:
  - table_name: "schema.table_name"
    execution_date: '{{ data_interval_end.strftime("%Y-%m-%d") }}'
    poke_interval: 60
    timeout: 600
    function: "tmp.fn_check_successful_execution"  # custom checker function
  
  - table_name: "schema.table_name_2"
    execution_date: '{{ (data_interval_end + macros.dateutil.relativedelta.relativedelta(hours=5)).strftime("%Y-%m-%d") }}'
    timeout: 300
```

### Procedures

#### Simple Procedures

Execute procedures sequentially:

```yaml
procedures:
  - database: "DATABASE"
    schema: "etl"
    name: "procedure_1"
    params:                          # Optional: procedure parameters
      param1: "value1"
      param2: 123
  
  - database: "DATABASE"
    schema: "etl"
    name: "procedure_2"
```

#### Task Groups

Group related procedures together:

```yaml
procedures:
  - task_group: "daily_load"
    procedures:
      - database: "DATABASE"
        schema: "etl"
        name: "load_daily_data"
      
      - database: "DATABASE"
        schema: "etl"
        name: "some_metrics"
```

#### Conditional Task Groups

Execute procedures only when conditions are met:

```yaml
procedures:
  # Run only on the 1st day of the month
  - task_group: "monthly_reports"
    condition: "execution_date.day == 1"
    procedures:
      - database: "DATABASE"
        schema: "reports"
        name: "generate_monthly_report"
  
  # Run only on Mondays
  - task_group: "weekly_cleanup"
    condition: "execution_date.isoweekday() == 1"
    procedures:
      - database: "DATABASE"
        schema: "maintenance"
        name: "cleanup_old_data"
  
  # Run only on the first Monday of the month
  - task_group: "first_monday"
    condition: "execution_date.day <= 7 and execution_date.isoweekday() == 1"
    procedures:
      - database: "DATABASE"
        schema: "reports"
        name: "special_report"
```

### Condition Examples

Available condition variables:
- `execution_date` - datetime object of the DAG run
- `datetime` - Python datetime module

Common condition patterns:

```python
# Day of month
"execution_date.day == 1"                    # First day of month
"execution_date.day == 15"                   # 15th day of month

# Day of week (Monday=1, Sunday=7)
"execution_date.isoweekday() == 1"           # Monday
"execution_date.isoweekday() == 5"           # Friday
"execution_date.isoweekday() in [6, 7]"      # Weekend

# Month
"execution_date.month == 1"                  # January
"execution_date.month in [1, 4, 7, 10]"      # Quarterly (Q1, Q2, Q3, Q4)

# Combined conditions
"execution_date.day == 1 and execution_date.month == 1"  # First day of year
"execution_date.day <= 7 and execution_date.isoweekday() == 1"  # First Monday
```

## Example: Complex DAG

```yaml
dag_id: "complex_etl_pipeline"
version: "v2.0.0"
schedule: "0 6 * * *"
start_date: "2026-01-01"
tags: ["dwh", "production"]
catchup: false
retries: 2
retry_delay_minutes: 5
execution_mode: "continue_on_failure"

alerts:
  on_failure: true
  on_sla_miss: true

wait_for:
  - external_dag_id: "source_data_ingestion"
    external_task_id: "end"
    execution_delta_hours: 2
    timeout: 1800

wait_for_tables:
  - table_name: "staging.raw_transactions"
    execution_date: '{{ data_interval_end.strftime("%Y-%m-%d") }}'
    timeout: 600

procedures:
  # Daily procedures - always run
  - task_group: "daily_etl"
    procedures:
      - database: "DATABASE"
        schema: "etl"
        name: "load_transactions"
      
      - database: "DATABASE"
        schema: "etl"
        name: "calculate_metrics"
  
  # Weekly procedures - run on Mondays
  - task_group: "weekly_aggregation"
    condition: "execution_date.isoweekday() == 1"
    procedures:
      - database: "DATABASE"
        schema: "reports"
        name: "aggregate_weekly_stats"
  
  # Monthly procedures - run on 1st of month
  - task_group: "monthly_closing"
    condition: "execution_date.day == 1"
    procedures:
      - database: "DATABASE"
        schema: "finance"
        name: "close_previous_month"
      
      - database: "DATABASE"
        schema: "reports"
        name: "generate_monthly_report"
```

## DAG Structure

Each generated DAG follows this pattern:

```
start → [sensors] → [procedures/task_groups] → end
```

- **start**: Empty operator marking DAG start
- **sensors**: External DAG sensors and/or table sensors (if configured)
- **procedures**: Sequential execution of stored procedures or task groups
- **end**: Final task that checks overall DAG status

## Custom Plugins Required

This factory requires two custom plugin modules:

### `custom_plugins/tg_notifications.py`

Should contain:
- `send_telegram_alert(context)` - sends failure notifications
- `send_telegram_sla_alert(dag, task_list, blocking_task_list, slas, blocking_tis)` - sends SLA miss notifications

### `custom_plugins/mssql_utils.py`

Should contain:
- `call_procedure(procedure_name, params, **context)` - executes MSSQL stored procedures

Example implementation:

```python
# mssql_utils.py
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

def call_procedure(procedure_name, params=None, **context):
    hook = MsSqlHook(mssql_conn_id="ms_sql_DATABASE")
    
    if params:
        param_str = ", ".join([f"@{k}='{v}'" for k, v in params.items()])
        sql = f"EXEC {procedure_name} {param_str}"
    else:
        sql = f"EXEC {procedure_name}"
    
    hook.run(sql)
    print(f"Successfully executed: {procedure_name}")
```

## Troubleshooting

### DAG not appearing in Airflow UI

1. Check Airflow logs for parsing errors
2. Verify `airflow_projects_path` variable is set correctly
3. Ensure YAML file is in the correct directory (`mssql_factory/*.yaml`)
4. Validate YAML syntax

### Sensors timing out

1. Increase `timeout` value in configuration
2. Check if upstream DAG/table actually exists and is being updated
3. Verify MSSQL connection is working
4. For table sensors, ensure the checker function returns correct results

### Tasks not running with conditions

1. Verify condition syntax is valid Python
2. Check that `execution_date` is being used correctly
3. Test condition logic independently
4. Review Airflow logs for condition evaluation errors

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue on GitHub.