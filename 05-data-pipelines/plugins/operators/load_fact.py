import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from ..helpers import LoadOperatorMode, table_to_query_map

class LoadFactOperator(BaseOperator):
    """
        LoadFactOperator loads data from staging area to fact tables.

        Operator could work in two modes:
        1. Table mode.
            It can load data to table using predefined query. To use operator this way you need to provide
            two required parameters:
                - operator_mode=LoadOperatorMode.Table
                - table='songplays'
        2. Query mode.
            In this mode you can provide custom query to load data wherever you want. To use operator this way
            you need to provide two required parameters:
                - operator_mode=LoadOperatorMode.Query
                - query='INSERT INTO ...' - your custom query.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 operator_mode=LoadOperatorMode.Table,
                 query='',
                 table='',
                 *args, **kwargs):
        """LoadFactOperator constructor. Defines the parameters required for the operator."""

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.operator_mode = operator_mode
        self.query = query
        self.table = table

    def execute(self, context):
        """Load fact table from staging."""

        try:
            self.log.info('Initialing Postgres hook (for Redshift)')
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

            if self.operator_mode == LoadOperatorMode.Table:
                sql = table_to_query_map[self.table]
                self.log.info(f'Loading data from staging to table "{self.table}"')
            else:
                sql = self.query
                self.log.info(f'Loading data using custom query:\n{self.query}')

            redshift.run(sql)
            self.log.info(f'Load operation successfully completed')
        except psycopg2.Error as e:
            self.log.error(f'Error occurred during during LOAD operation: {e}')
            raise
