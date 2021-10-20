from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 dq_checks='',
                 redshift_conn_id='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        error_count = 0
        
        for i in self.dq_checks:
            sql = i.get('check_sql')
            exp_result = i.get('expected_result')
            
            self.log.info(f'Running {sql}')
            records = redshift_hook.get_records(sql)[0]  
            
            if exp_result != records[0]:
                error_count += 1
                self.log.info(f'Test failed {sql}')
        
        if error_count ==  0:
            self.log.info("All data quality checks passed")
            
        
        