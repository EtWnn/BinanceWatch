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


SPOT_WITHDRAW_TABLE = Table(
    'spot_withdraw',
    [
        'id',
        'txId',
        'applyTime',
        'asset',
        'amount',
        'fee'
    ],
    [
        'TEXT',
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL',
        'REAL'
    ]
)

SPOT_DIVIDEND_TABLE = Table(
    'spot_dividend_table',
    [
        'id',
        'divTime',
        'asset',
        'amount'
    ],
    [
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL'
    ]
)
