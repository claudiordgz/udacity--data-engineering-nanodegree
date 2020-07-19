from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    insert_queries = {
        'artists': SqlQueries.artist_table_insert,
        'songs': SqlQueries.song_table_insert,
        'users': SqlQueries.user_table_insert,
        'time': SqlQueries.time_table_insert
    }

    @apply_defaults
    def __init__(self,
                redshift_conn_id="redshift",
                dimension_name="",
                append_only=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_name = dimension_name

    def execute(self, context):
        self.log.info("LoadDimensionOperator: Starting execution")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.dimension_name not in self.insert_queries:
            raise AttributeError("dimension_name must be one of: artists, songs, users, or time")
        
        formatted_sql = None
        if self.dimension_name == 'time':
            formatted_sql = f"DROP TABLE IF EXISTS {self.dimension_name};" if not self.append_only else ""
            formatted_sql = f"{formatted_sql}\nCREATE TABLE time AS ({self.insert_queries[self.dimension_name]})"
        else:
            formatted_sql = f"DELETE {self.dimension_name};" if not self.append_only else ""
            formatted_sql = f"{formatted_sql}\nINSERT INTO {self.dimension_name} ({self.insert_queries[self.dimension_name]})"
        redshift_hook.run(formatted_sql)
        self.log.info("LoadDimensionOperator: Finished execution")
