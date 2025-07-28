from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION 'us-west-2'
        COMPUPDATE OFF
        STATUPDATE OFF
        TRUNCATECOLUMNS
        MAXERROR 1000
    """

    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path

    def execute(self, context):
        self.log.info("Getting AWS credentials from connection")
        aws_connection = BaseHook.get_connection(self.aws_credentials_id)
        access_key = aws_connection.login
        secret_key = aws_connection.password
        
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            access_key,
            secret_key,
            self.json_path
        )
        
        self.log.info(f"Executing COPY command: {formatted_sql}")
        
        try:
            redshift.run(formatted_sql)
            self.log.info(f"Successfully copied data from {s3_path} to {self.table}")
        except Exception as e:
            self.log.error(f"COPY command failed: {str(e)}")
            self.log.info("Checking STL_LOAD_ERRORS for detailed error information...")
            
            error_query = """
                SELECT * FROM stl_load_errors 
                WHERE session = (SELECT MAX(session) FROM stl_load_errors)
                ORDER BY starttime DESC 
                LIMIT 10
            """
            
            try:
                errors = redshift.get_records(error_query)
                if errors:
                    self.log.error("Load errors found:")
                    for error in errors:
                        self.log.error(f"Error: {error}")
                else:
                    self.log.info("No load errors found in STL_LOAD_ERRORS")
            except Exception as error_check_e:
                self.log.error(f"Could not check load errors: {str(error_check_e)}")
            
            raise e





