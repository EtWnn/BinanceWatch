from pandas import DataFrame

from src.storage.DataBase import DataBase, SQLConditionEnum
from src.storage.tables import Table

db = DataBase("test_table")

table1 = Table(
    'test_table1',
    [
        'age',
        'surname',
        'weight'
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL'
    ],
    primary_key='key',
    primary_key_sql_type='INTEGER'
)

table2 = Table(
    'test_table2',
    [
        'age',
        'surname',
        'weight'
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL'
    ],
)


def test_create_cmd(verbose=0, **kwargs):
    create_cmd = db.get_create_cmd(table1)
    if verbose:
        print(create_cmd)
    assert create_cmd == "CREATE TABLE test_table1\n([key] INTEGER PRIMARY KEY, [age] INTEGER, [surname] TEXT," \
                         " [weight] REAL)"

    create_cmd = db.get_create_cmd(table2)
    if verbose:
        print(create_cmd)
    assert create_cmd == "CREATE TABLE test_table2\n([age] INTEGER, [surname] TEXT, [weight] REAL)"


def test_inserts_search_1(verbose=0, **kwargs):
    db.drop_table(table1)
    rows = [
        (1, 15, 'Karl', 55.5),
        (2, 18, 'Kitty', 61.1),
        (3, 18, 'Marc', 48.1),
        (8, 55, 'Jean', 78.1)
    ]
    db.add_rows(table1, rows)

    retrieved_row = db.get_row_by_key(table1, 1)
    if verbose:
        print(f"retrieved the row: {retrieved_row} when looking for the row: {rows[0]}")
    assert rows[0] == retrieved_row

    conditions = [
        (table1.age, SQLConditionEnum.equal, 18),
        (table1.weight, SQLConditionEnum.greater_equal, 55),
    ]
    retrieved_rows = db.get_conditions_rows(table1, conditions_list=conditions)
    assert rows[1:2] == retrieved_rows

    # check max weight is right
    assert max([r[-1] for r in rows]) == db.get_conditions_rows(table1, selection=f"MAX({table1.weight})")[0][0]


def test_inserts_search_2(verbose=0, **kwargs):
    db.drop_table(table2)
    rows = [
        (15, 'Karl', 55.5),
        (18, 'Kitty', 61.1),
        (18, 'Kitty', 61.1),  # no set primary key -> should allow identical rows
        (18, 'Marcus', 58.2),
        (18, 'Marc', 48.1),
        (55, 'Jean', 78.1)
    ]
    db.add_rows(table2, rows)

    try:
        db.get_row_by_key(table2, 1)
        raise RuntimeError("the above line should throw an error as no primary key is defined")
    except ValueError:
        pass

    conditions = [
        (table2.age, SQLConditionEnum.equal, 18),
        (table2.weight, SQLConditionEnum.greater_equal, 55),
    ]
    order_list = [table2.weight]
    retrieved_rows = db.get_conditions_rows(table2, conditions_list=conditions, order_list=order_list)
    sub_rows = list(rows[1:4])
    if verbose:
        print("rows retrieved:")
        for row in retrieved_rows:
            print('\t', row)
    sub_rows.sort(key=lambda x: x[-1])
    assert sub_rows == retrieved_rows

    # check min ge is right
    assert max([r[0] for r in rows]) == db.get_conditions_rows(table2, selection=f"MAX({table2.age})")[0][0]
