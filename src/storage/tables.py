from dataclasses import dataclass
from typing import List


@dataclass
class Table:
    name: str
    columns_names: List[str]
    columns_sql_types: List[str]


SPOT_TRADE_TABLE = Table(
    'spot_trade',
    [
        'key',
        'id',
        'millistamp',
        'asset',
        'ref_asset',
        'qty',
        'price',
        'fee',
        'fee_asset',
        'isBuyer'

    ],
    [
        'TEXT',
        'INTEGER',
        'INTEGER',
        'TEXT',
        'TEXT',
        'REAL',
        'REAL',
        'REAL',
        'TEXT',
        'INTEGER'
    ]
)

SPOT_DEPOSIT_TABLE = Table(
    'spot_deposit',
    [
        'txId',
        'insertTime',
        'asset',
        'amount',
    ],
    [
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL'
    ]
)
