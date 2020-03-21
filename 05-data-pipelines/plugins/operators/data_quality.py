import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import DataQualityCheckBase


class DataQualityOperator(BaseOperator):
    """
        DataQualityOperator runs data quality checks.
        The main goal is to ensure the correctness, meaningfulness and integrity of loaded data.

        The idea of the operator implementation is to allow to the caller pass both checks with custom query
        and expected result for it or predefined quality checks. In the same time we have to keep code clean
        and readable. Thus operator expected input in "data_quality_checks" parameter is an array of objects
        which implements DataQualityCheckBase interface. See "..helpers.data_quality_checks.py" for more details.
        It gives us benefits:
            - caller code will be cleaner, because it have to pass instances of predefined classes which encapsulates
                query and expected results;
            - operator code is very clean and readable and works with any checks in unified manner.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 data_quality_checks=None,
                 *args, **kwargs):
        """DataQualityOperator constructor. Defines the parameters required for the operator."""

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        """Run data quality checks to ensure that loaded data is correct and meaningful."""

        try:
            if not self.data_quality_checks:
                return

            self.log.info('Initialing Postgres hook (for Redshift)')
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            for check in self.data_quality_checks:
                assert issubclass(check, DataQualityCheckBase), \
                    f'Wrong object "{check}" for data quality check. It should be inherited from DataQualityCheckBase.'

                self.log.info(f'Data quality check of type "{check.type}". Query:\n{check.query}')
                actual_result = redshift.run(check.query)
                expected_result = check.expected_result

                assert expected_result == actual_result, \
                    f'Data quality check type: "{check.type}", expected: {expected_result}, actual: {actual_result}'

        except psycopg2.Error as e:
            self.log.error(f'Error occurred during during LOAD operation: {e}')
            raise
        except AssertionError as e:
            self.log.error(f'Data quality check error: {e}')
            raise
