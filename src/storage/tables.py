from dataclasses import dataclass
from typing import List


@dataclass
class Table:
    name: str
    columns_names: List[str]
    columns_sql_types: List[str]


BINANCE_SPOT_TRADE_TABLE = Table(
    'spot_trade',
    [
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
