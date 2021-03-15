from dataclasses import dataclass
from typing import List, Optional


class Table:
    """
    @DynamicAttrs
    """

    def __init__(self, name: str, columns_names: List[str], columns_sql_types: List[str],
                 primary_key: Optional[str] = None, primary_key_sql_type: Optional[str] = None):
        self.name = name
        self.columns_names = columns_names
        self.columns_sql_types = columns_sql_types
        self.primary_key = primary_key
        self.primary_key_sql_type = primary_key_sql_type

        for column_name in self.columns_names:
            try:
                value = getattr(self, column_name)
                raise ValueError(f"the name {column_name} conflicts with an existing attribute of value {value}")
            except AttributeError:
                setattr(self, column_name, column_name)

        if self.primary_key is not None:
            setattr(self, self.primary_key, self.primary_key)


SPOT_TRADE_TABLE = Table(
    'spot_trade',
    [
        'millistamp',
        'asset',
        'refAsset',
        'qty',
        'price',
        'fee',
        'feeAsset',
        'isBuyer'

    ],
    [
        'INTEGER',
        'TEXT',
        'TEXT',
        'REAL',
        'REAL',
        'REAL',
        'TEXT',
        'INTEGER'
    ],
    primary_key='tradeId',
    primary_key_sql_type='INTEGER'
)

SPOT_DEPOSIT_TABLE = Table(
    'spot_deposit',
    [
        'insertTime',
        'asset',
        'amount',
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL'
    ],
    primary_key='txId',
    primary_key_sql_type='TEXT'
)


SPOT_WITHDRAW_TABLE = Table(
    'spot_withdraw',
    [
        'txId',
        'applyTime',
        'asset',
        'amount',
        'fee'
    ],
    [
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL',
        'REAL'
    ],
    primary_key='withdrawId',
    primary_key_sql_type='TEXT'
)

SPOT_DIVIDEND_TABLE = Table(
    'spot_dividend_table',
    [
        'divTime',
        'asset',
        'amount'
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL'
    ],
    primary_key='divId',
    primary_key_sql_type='INTEGER'
)

SPOT_DUST_TABLE = Table(
    'spot_dust_table',
    [
        'dustId',
        'time',
        'asset',
        'assetAmount',
        'bnbAmount',
        'bnbFee',
    ],
    [
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL',
        'REAL',
        'REAL'
    ]
)

LENDING_INTEREST_TABLE = Table(
    'lending_interest_table',
    [
        'time',
        'lendingType',
        'asset',
        'amount',
    ],
    [
        'INTEGER',
        'TEXT',
        'TEXT',
        'REAL',
    ]
)
