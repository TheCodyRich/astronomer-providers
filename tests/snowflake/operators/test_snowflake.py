import datetime
from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from airflow.models.dag import DAG

from astronomer.providers.snowflake.hooks.snowflake import SnowflakeHookAsync
from astronomer.providers.snowflake.operators.snowflake import (
    SnowflakeOperatorAsync,
    SnowflakeSqlApiOperatorAsync,
)
from astronomer.providers.snowflake.triggers.snowflake_trigger import (
    SnowflakeSqlApiTrigger,
    SnowflakeTrigger,
)
from tests.utils.airflow_util import create_context

MODULE = "astronomer.providers.snowflake"
TASK_ID = "snowflake_check"
CONN_ID = "my_snowflake_conn"
TEST_SQL = "select * from any;"

SQL_MULTIPLE_STMTS = (
    "create or replace table user_test (i int); insert into user_test (i) "
    "values (200); insert into user_test (i) values (300); select i from user_test order by i;"
)

SINGLE_STMT = "select i from user_test order by i;"


class TestSnowflakeOperatorAsync:
    @pytest.mark.parametrize("mock_sql", [TEST_SQL, [TEST_SQL]])
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    def test_snowflake_execute_operator_async(self, mock_db_hook, mock_sql):
        """
        Asserts that a task is deferred and an SnowflakeTrigger will be fired
        when the SnowflakeOperatorAsync is executed.
        """
        dag = DAG("test_snowflake_async_execute_complete_failure", start_date=datetime.datetime(2023, 1, 1))
        operator = SnowflakeOperatorAsync(
            task_id="execute_run",
            snowflake_conn_id=CONN_ID,
            dag=dag,
            sql=mock_sql,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(exc.value.trigger, SnowflakeTrigger), "Trigger is not a SnowflakeTrigger"

    def test_snowflake_async_execute_complete_failure(self):
        """Tests that an AirflowException is raised in case of error event"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )
        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=None,
                event={"status": "error", "message": "Test failure message", "type": ""},
            )

    @pytest.mark.parametrize(
        "mock_event, mock_xcom_push",
        [
            ({"status": "success", "query_ids": ["uuid", "uuid"]}, True),
            ({"status": "success", "query_ids": ["uuid", "uuid"]}, False),
        ],
    )
    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    def test_snowflake_async_execute_complete(self, mock_conn, mock_event, mock_xcom_push):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
            do_xcom_push=mock_xcom_push,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", "execute_complete")

    @mock.patch(f"{MODULE}.operators.snowflake.SnowflakeOperatorAsync.get_db_hook")
    def test_snowflake_sql_api_execute_complete_event_none(self, mock_conn):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )

        with pytest.raises(AirflowException):
            operator.execute_complete(context=None, event=None)

    def test_get_db_hook(self):
        """Test get_db_hook with async hook"""

        operator = SnowflakeOperatorAsync(
            task_id="execute_complete",
            snowflake_conn_id=CONN_ID,
            sql=TEST_SQL,
        )
        result = operator.get_db_hook()
        assert isinstance(result, SnowflakeHookAsync)


class TestSnowflakeSqlApiOperatorAsync:
    @pytest.mark.parametrize("mock_sql, statement_count", [(SQL_MULTIPLE_STMTS, 4), (SINGLE_STMT, 1)])
    @mock.patch(
        "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.execute_query"
    )
    def test_snowflake_sql_api_execute_operator_async(self, mock_db_hook, mock_sql, statement_count):
        """
        Asserts that a task is deferred and an SnowflakeSqlApiTrigger will be fired
        when the SnowflakeSqlApiOperatorAsync is executed.
        """
        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=mock_sql,
            statement_count=statement_count,
        )

        with pytest.raises(TaskDeferred) as exc:
            operator.execute(create_context(operator))

        assert isinstance(
            exc.value.trigger, SnowflakeSqlApiTrigger
        ), "Trigger is not a SnowflakeSqlApiTrigger"

    def test_snowflake_sql_api_execute_complete_failure(self):
        """Test SnowflakeSqlApiOperatorAsync raise AirflowException of error event"""

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
        )
        with pytest.raises(AirflowException):
            operator.execute_complete(
                context=None,
                event={"status": "error", "message": "Test failure message", "type": "FAILED_WITH_ERROR"},
            )

    @pytest.mark.parametrize(
        "mock_event",
        [
            None,
            ({"status": "success", "statement_query_ids": ["uuid", "uuid"]}),
        ],
    )
    @mock.patch(
        "astronomer.providers.snowflake.hooks.snowflake_sql_api.SnowflakeSqlApiHookAsync.check_query_output"
    )
    def test_snowflake_sql_api_execute_complete(self, mock_conn, mock_event):
        """Tests execute_complete assert with successful message"""

        operator = SnowflakeSqlApiOperatorAsync(
            task_id=TASK_ID,
            snowflake_conn_id=CONN_ID,
            sql=SQL_MULTIPLE_STMTS,
            statement_count=4,
        )

        with mock.patch.object(operator.log, "info") as mock_log_info:
            operator.execute_complete(context=None, event=mock_event)
        mock_log_info.assert_called_with("%s completed successfully.", TASK_ID)
