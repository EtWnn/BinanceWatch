import sys
import os
from enum import Enum
from typing import List, Tuple, Optional, Any
import sqlite3

from src.storage.tables import Table
from src.utils.LoggerGenerator import LoggerGenerator


class SQLConditionEnum(Enum):
    equal = '='
    greater_equal = '>='
    greater = '>'
    lower = '<'
    lower_equal = '<='


class DataBase:
    """
    This class will be used to interact with sqlite3 databases without having to generates sqlite commands
    """

    def __init__(self, name: str):
        self.name = name
        self.logger = LoggerGenerator.get_logger(self.name)
        self.save_path = f"data/{name}.db"
        self.db_conn = sqlite3.connect(self.save_path)
        self.db_cursor = self.db_conn.cursor()

    def _fetch_rows(self, execution_cmd: str):
        """
        execute a command to fetch some rows and return them
        :param execution_cmd: the command to execute
        :return:
        """
        rows = []
        try:
            self.db_cursor.execute(execution_cmd)
        except sqlite3.OperationalError:
            return rows
        while True:
            row = self.db_cursor.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def get_row_by_key(self, table: Table, key_value) -> Optional[Tuple]:
        """
        get the row identified by a primary key value from a table
        :param table: table to fetch the row from
        :param key_value: key value of the row
        :return: None or the row of value
        """
        conditions_list = [(table.columns_names[0], SQLConditionEnum.equal, key_value)]
        rows = self.get_conditions_rows(table, conditions_list)
        if len(rows):
            return rows[0]

    def get_conditions_rows(self, table: Table,
                            conditions_list: Optional[List[Tuple[str, SQLConditionEnum, Any]]] = None) -> List:
        if conditions_list is None:
            conditions_list = []
        execution_cmd = f"SELECT * from {table.name}"
        execution_cmd = self._add_conditions(execution_cmd, conditions_list)
        return self._fetch_rows(execution_cmd)

    def get_all_rows(self, table: Table) -> List:
        return self.get_conditions_rows(table)

    def add_row(self, table: Table, row: Tuple, auto_commit: bool = True, update_if_exists: bool = False):
        row_s = ", ".join(f"'{v}'" for v in row)
        row_s = f'({row_s})'
        execution_order = f"INSERT INTO {table.name} VALUES {row_s}"
        try:
            self.db_cursor.execute(execution_order)
            if auto_commit:
                self.commit()
        except sqlite3.OperationalError:
            self.create_table(table)
            self.db_cursor.execute(execution_order)
            if auto_commit:
                self.commit()
        except sqlite3.IntegrityError as err:
            if update_if_exists:
                self.update_row(table, row, auto_commit)
            else:
                raise err

    def add_rows(self, table: Table, rows: List[Tuple], auto_commit: bool = True, update_if_exists: bool = False):
        for row in rows:
            self.add_row(table, row, auto_commit=False, update_if_exists=update_if_exists)
        if auto_commit:
            self.commit()

    def update_row(self, table: Table, row: Tuple, auto_commit=True):
        row_s = ", ".join(f"{n} = {v}" for n, v in zip(table.columns_names[1:], row[1:]))
        execution_order = f"UPDATE {table.name} SET {row_s} WHERE {table.columns_names[0]} = {row[0]}"
        self.db_cursor.execute(execution_order)
        if auto_commit:
            self.commit()

    def create_table(self, table: Table):
        """
        create a table in the database
        :param table: Table instance with the config of the table to create
        :return:
        """
        create_cmd = self.get_create_cmd(table)
        self.db_cursor.execute(create_cmd)
        self.db_conn.commit()

    def drop_table(self, table: Table):
        """
        delete a table from the database
        :param table: Table instance with the config of the table to drop
        :return:
        """
        execution_order = f"DROP TABLE IF EXISTS {table.name}"
        self.db_cursor.execute(execution_order)
        self.db_conn.commit()

    def commit(self):
        """
        submit and save the database state
        :return:
        """
        self.db_conn.commit()

    @staticmethod
    def _add_conditions(execution_cmd: str, conditions_list: List[Tuple[str, SQLConditionEnum, Any]]):
        """
        add a list of condition to an SQL command
        :param execution_cmd: string with 'WHERE' statement
        :param conditions_list:
        :return:
        """
        if len(conditions_list):
            add_cmd = ' WHERE'
            for column_name, condition, value in conditions_list:
                add_cmd = add_cmd + f" {column_name} {condition.value} '{value}' AND"
            return execution_cmd + add_cmd[:-4]
        else:
            return execution_cmd

    @staticmethod
    def get_create_cmd(table: Table):
        """
        return the command in string format to create a table in the database
        :param table: Table instance with the config if the table to create
        :return: execution command for the table creation
        """
        cmd = f"[{table.columns_names[0]}] {table.columns_sql_types[0]} PRIMARY KEY, "
        for arg_name, arg_type in zip(table.columns_names[1:], table.columns_sql_types[1:]):
            cmd = cmd + f"[{arg_name}] {arg_type}, "
        return f"CREATE TABLE {table.name}\n({cmd[:-2]})"
