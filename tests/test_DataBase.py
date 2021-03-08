from src.storage.DataBase import DataBase, SQLConditionEnum
from src.storage.tables import Table

table = Table(
    'test_table',
    [
        'Key',
        'age',
        'name',
        'weight'
    ],
    [
        'INTEGER',
        'INTEGER',
        'TEXT',
        'REAL'
    ]
)


def test_inserts_search(verbose=0, **kwargs):
    db = DataBase("test_table")
    db.drop_table(table)
    rows = [
        (1, 15, 'Karl', 55.5),
        (2, 18, 'Kitty', 61.1),
        (3, 18, 'Marc', 48.1),
        (8, 55, 'Jean', 78.1)
    ]
    db.add_rows(table, rows)

    retrieved_row = db.get_row_by_key(table, 1)
    if verbose:
        print(f"retrieved the row: {retrieved_row} when looking for the row: {rows[0]}")
    assert rows[0] == retrieved_row

    conditions = [
        (table.columns_names[1], SQLConditionEnum.equal, 18),
        (table.columns_names[3], SQLConditionEnum.greater_equal, 55),
    ]
    retrieved_rows = db.get_conditions_rows(table, conditions)
    assert rows[1:2] == retrieved_rows
