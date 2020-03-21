from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

from .operators import (
    StageToRedshiftOperator,
    LoadDimensionOperator,
    LoadFactOperator,
    DataQualityOperator
)
from .helpers import (
    table_to_query_map,
    LoadOperatorMode,
    SaveMode,
    DataQualityCheckBase,
    DataQualityCheckType,
    TableIsNotEmptyDataQualityCheck,
    ColumnDoesNotContainNullsDataQualityCheck,
    CustomDataQualityCheck
)


# Defining the plugin class
class SparkifyPlugin(AirflowPlugin):
    name = 'sparkify_plugin'

    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]

    helpers = [
        helpers.table_to_query_map,
        helpers.LoadOperatorMode,
        helpers.SaveMode,
        helpers.DataQualityCheckBase,
        helpers.DataQualityCheckType,
        helpers.TableIsNotEmptyDataQualityCheck,
        helpers.ColumnDoesNotContainNullsDataQualityCheck,
        helpers.CustomDataQualityCheck
    ]
