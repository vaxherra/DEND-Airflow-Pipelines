from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data Quality operator. Connects to a data-warehouse through provided connection ID, and loops over provided set of tests, each specifying an SQL query and a condition string to be evaluated.
    Operator logs passes and raises a `ValueError` on the first occurence of an error.
    
    Args:
        redshift_conn_id    : an Airflow conn_id for Redshift
        tests               : a list containing tuples (SQL_statement, test), where:
            - SQL_statement : an sql statement, results of which would be compared to the test statement
            - test          : a string containing "{} condition", where "{}" is auto-filled, and contains the results on an SQL_statement, and "condition" is an evaluable expression, ex. quality operator "==". See `README.md` in the main file for an example
       
    Returns:
        None
        
       
 
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.tests=tests

    def execute(self, context):
 
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
    
        for i,(sql_test,condition) in enumerate(self.tests):   
            sql_test_records = redshift.get_records(sql_test)
            test = eval( condition.format(sql_test_records)   )
            
            if(test):
                self.log.info( str("[") + str(i+1) + str("/") + str(len(self.tests))   +  "] test passed. \n\n SQL STATEMENT: " + str(sql_test) + "\n\n Test Passed, because: \n\n " +  condition.format(sql_test_records)   )
            else:
                raise ValueError(f"Data quality check failed. Expected value different than obtained:  " + condition.format(sql_test_records) )
                
            
 
        
        