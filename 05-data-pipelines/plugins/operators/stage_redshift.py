from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
        StageToRedshiftOperator allows to COPY data from S3 storage to Redshift table.
        Operator allows to pass template for the S3_KEY to use with various S3 path.
    """

    ui_color = '#358140'
    template_fields = ['s3_key']

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 aws_region='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 *args, **kwargs):
        """StageToRedshiftOperator operator constructor. Defines the parameters required for the operator."""

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.aws_region = aws_region
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        """Prepare parameters for COPY SQL statement and invoke COPY operation from S3 to Redshift"""

        self.log.info('Initialing AWS hook')
        aws = AwsHook(self.aws_credentials_id)
        aws_credentials = aws.get_credentials(region_name=self.aws_region)

        self.log.info('Initialing Postgres hook (for Redshift)')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Clearing data from destination Redshift table')
        redshift.run(f'DELETE FROM {self.table};')

        self.log.info('Copying data from S3 to Redshift...')
        s3_rendered_key = self.s3_key.format(**context)
        s3_data_path = f's3://{self.s3_bucket}/{s3_rendered_key}'
        formatted_sql = self.get_formatted_copy_sql(
            table=self.table,
            s3_data_path=s3_data_path,
            aws_key=aws_credentials.access_key,
            aws_secret=aws_credentials.secret_key,
            aws_region=self.aws_region)
        redshift.run(formatted_sql)
        self.log.info(f'Copying from S3 to Redshift table "{self.table}" successfully completed!')

    @staticmethod
    def get_formatted_copy_sql(table, s3_data_path, aws_key, aws_secret, aws_region):
        """Create formatted COPY SQL statement from the template"""

        return f"""
            COPY {table}
            FROM {s3_data_path}
            ACCESS_KEY_ID {aws_key}
            SECRET_ACCESS_KEY {aws_secret}
            FORMAT AS JSON 'auto'
            REGION {aws_region};
        """
