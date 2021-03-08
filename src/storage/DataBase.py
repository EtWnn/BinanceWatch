import traceback
from typing import List, Tuple, Optional
import sqlite3

from src.storage.Tables import Table
from src.utils.LoggerGenerator import LoggerGenerator


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

    def get_row(self, table: Table, primary_value) -> Optional[Tuple]:
        """
        get the row identified by a primary key value from a table
        :param table: table to fetch the row from
        :param primary_value: key value of the row
        :return: None or the row of value
        """
        try:
            cmd = f"SELECT * from {table.name} WHERE {table.columns_names[0]} = {primary_value}"
            self.db_cursor.execute(cmd)
        except sqlite3.OperationalError:
            return None
        return self.db_cursor.fetchone()

    def get_range_rows(self, table: Table, column_num: int, low_value: float, high_value: float) -> List:
        try:
            column_name = table.columns_names[column_num]
        except IndexError:
            msg = f"column number {column_num} was asked but the table has only the columns {table.columns_names}"
            self.logger.error(msg)
            return []
        rows = []
        args = (low_value, high_value,)
        try:
            cmd = f"SELECT * from {table.name} WHERE {column_name} >= ? AND {column_name} < ?"
            self.db_cursor.execute(cmd, args)
        except sqlite3.OperationalError:
            return rows
        while True:
            row = self.db_cursor.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def get_all_rows(self, table: Table) -> List:
        rows = []
        try:
            cmd = f"SELECT * from {table.name}"
            self.db_cursor.execute(cmd)
        except sqlite3.OperationalError:
            self.logger.error(str(traceback.format_exc()))
            return rows
        while True:
            row = self.db_cursor.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def add_row(self, table: Table, row: Tuple, auto_commit: bool = True, update_if_exists: bool = False):
        row_s = ", ".join(str(v) for v in row)
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

    def update_row(self, table: Table, row: Tuple, auto_commit=True):
        row_s = ", ".join(f"{n} = {v}" for n, v in zip(table.columns_names[1:], row[1:]))
        execution_order = f"UPDATE {table.name} SET {row_s} WHERE {table.columns_names[0]} = {row[0]}"
        self.db_cursor.execute(execution_order)
        if auto_commit:
            self.commit()

    def create_table(self, table: Table):
        create_cmd = self.get_create_cmd(table)
        self.db_cursor.execute(create_cmd)
        self.db_conn.commit()

    def drop_table(self, table: Table):
        execution_order = f"DROP TABLE IF EXISTS {table.name}"
        self.db_cursor.execute(execution_order)
        self.db_conn.commit()

    def commit(self):
        self.db_conn.commit()

    @staticmethod
    def get_create_cmd(table: Table):
        cmd = f"[{table.columns_names[0]}] {table.columns_sql_types[0]} PRIMARY KEY, "
        for arg_name, arg_type in zip(table.columns_names[1:], table.columns_sql_types[1:]):
            cmd = cmd + f"[{arg_name}] {arg_type}, "
        return f"CREATE TABLE {table.name}\n({cmd[:-2]})"
