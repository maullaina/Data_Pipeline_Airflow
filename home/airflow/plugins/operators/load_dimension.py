from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {};
    """
    
    delete_sql = """
        DELETE FROM {}
        ;
    """
    
    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 append_data = False,
                 load_sql_stmt='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data,
        self.load_sql_stmt = load_sql_stmt
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.append_data == True:
            formatted_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.load_sql_stmt
            )
            redshift.run(formatted_sql)
        else:
            sql_statement = LoadDimensionOperator.delete_sql.format(
                self.table
            )
            redshift.run(sql_statement)

            formatted_sql = LoadDimensionOperator.insert_sql.format(
                self.table,
                self.load_sql_stmt
            )
            redshift.run(formatted_sql)