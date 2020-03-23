from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data from AWS Redshift staging tables to a specified dimension table.
    
    Args:
         redshift_conn_id   : an Airflow conn_id for Redshift
         target_table       : name of target dimension table in Redshift cluster
         target_columns     : string containing comma separated list of columns for the target Redshift table. Must be compatible with the resuling columns from the `query` parameter
         insert_mode        : "append" (default) or "delete_load". By default the new records are appended to existing ones without deleting all entries. Choose "delete_load" to pre-empty the dimension table before filling.
         query              : A string SQL statement to query the staging table, extracting desired columns and rows for the target table

    Returns:
        None
    
    """

    ui_color = '#80BD9E'
    
    # SQL template: INSERT INTO <target_table>(<target_columns>) <QUERY>
    sql_template = """
        INSERT INTO {} ({})
            ({});
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_table="",
                 target_columns="",
                 query="",
                 insert_mode="append",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.target_table=target_table
        self.target_columns=target_columns
        self.query=query
        self.insert_mode=insert_mode
        
    def execute(self, context):
        
        #Redshift hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Modes
        if self.insert_mode == "delete_load":
            self.log.info("deleta_load mode: proceeding to clear data from Redshift target table: {} ..."\
                            .format(self.target_table))
            redshift.run("DELETE FROM {}".format(self.target_table))
 
          
        elif self.insert_mode == "append":
            self.log.info("append mode: injecting new data on top of old one in Redshift target table: {} ..."\
                            .format(self.target_table))
        else:
            self.log.info("Insert_mode not recognized/defined: defaulting to append (default value). Injecting new data on top of old one in Redshift target table: {} ..."\
                            .format(self.target_table))
        
        
 
        sql_query = self.sql_template.format(self.target_table,  self.target_columns,   self.query.format(self.target_table)    )
        
        self.log.info("Inserting...")
        redshift.run(sql_query)
        self.log.info("Finished.")

