from abc import ABC, abstractmethod
from enum import Enum


class DataQualityCheckType(Enum):
    TableIsNotEmpty = 1
    ColumnDoesNotContainNulls = 2
    Custom = 99


class DataQualityCheckBase(ABC):
    """
        DataQualityCheckBase defines the interface for data quality check classes.
        There are three required property:
        1. `type` - returns type of data quality check according to DataQualityCheckType enum
        2. `query` - returns query to get the actual value for the check
        3. `expected_result` - returns the expected value of the check
        If actual query result and expected result are equal then check is passed, otherwise - failed.
    """

    @property
    @abstractmethod
    def type(self):
        pass

    @property
    @abstractmethod
    def query(self):
        pass

    @property
    @abstractmethod
    def expected_result(self):
        pass


class TableIsNotEmptyDataQualityCheck(DataQualityCheckBase):
    def __init__(self, table_name):
        self.table_name = table_name

    @property
    def type(self):
        return DataQualityCheckType.TableIsNotEmpty

    @property
    def query(self):
        return f'SELECT COUNT(*) FROM {self.table_name};'

    @property
    def expected_result(self):
        return 0


class ColumnDoesNotContainNullsDataQualityCheck(DataQualityCheckBase):
    def __init__(self, table_name, column_name):
        self.table_name = table_name
        self.column_name = column_name

    @property
    def type(self):
        return DataQualityCheckType.ColumnDoesNotContainNulls

    @property
    def query(self):
        return f"""
            SELECT COUNT(*)
            FROM {self.table_name}
            WHERE {self.column_name} IS NULL;
        """

    @property
    def expected_result(self):
        return 0


class CustomDataQualityCheck(DataQualityCheckBase):
    def __init__(self, query, expected_result):
        self.query = query
        self.expected_result = expected_result

    @property
    def type(self):
        return DataQualityCheckType.Custom

    @property
    def query(self):
        return self.query

    @property
    def expected_result(self):
        return self.expected_result
