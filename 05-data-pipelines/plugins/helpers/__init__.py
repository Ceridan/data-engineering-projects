from .sql_queries import SqlQueries
from .load_operator_mode import LoadOperatorMode
from .save_mode import SaveMode
from .data_quality_checks import (
    DataQualityCheckType,
    DataQualityCheckBase,
    TableIsNotEmptyDataQualityCheck,
    ColumnDoesNotContainNullsDataQualityCheck,
    CustomDataQualityCheck)


table_to_query_map = {
    'songplays': SqlQueries.songplay_table_insert,
    'users': SqlQueries.user_table_insert,
    'songs': SqlQueries.song_table_insert,
    'artists': SqlQueries.artist_table_insert,
    'time': SqlQueries.time_table_insert,
}

__all__ = [
    'table_to_query_map',
    'LoadOperatorMode',
    'SaveMode',
    'DataQualityCheckBase',
    'DataQualityCheckType',
    'TableIsNotEmptyDataQualityCheck',
    'ColumnDoesNotContainNullsDataQualityCheck',
    'CustomDataQualityCheck',
]
