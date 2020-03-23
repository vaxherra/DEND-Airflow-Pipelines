from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Custom Airflow Operator to load any JSON formatted file residing in S3 bucket to Amazon Redshift warehouse. Operator runs an SQL copy statement based using provided AWS credentials and Airflow PostgresHook for a pre-configured connection.
    
    
    Args:
        redshift_conn_id         : an Airflow conn_id for Redshift 
        aws_credentials_id       : an Airflow conn_id for AWS user
        table                    : name of target staging table
        s3_bucket                : name of S3 bucket
        s3_key                   : name of S3 key 
        JSONPaths                :
    
    Returns:
        None
    """
    # Use "s3_key" as template, allowing to use context variables for formatting
    template_fields = ("s3_key",)
    
    ui_color = '#358140'
    
    # SQL copy template for subsequent dynamic filling
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        TIMEFORMAT AS 'epochmillisecs'
        region 'us-west-2'
    """

    # A constructor defining parameters to the operator
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 JSONPaths="",
                 *args, **kwargs):
        
        # Call parent constructor
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        # Map params to object
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.JSONPaths = JSONPaths

    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from staging Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift staging table")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)   
        json_path = "s3://{}/{}".format(self.s3_bucket, self.JSONPaths) 
        
        if rendered_key=="song_data": # song JSON data files don't have JSON manifest file that is why need to mark as Auto
            json_path = self.JSONPaths
            
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            json_path
        )
        redshift.run(formatted_sql)
