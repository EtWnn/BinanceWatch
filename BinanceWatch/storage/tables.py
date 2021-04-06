from dataclasses import dataclass
from typing import List, Optional


class Table:
    """
    This class represent a table in a database. All columns names are dynamic attributes
    @DynamicAttrs
    """

    def __init__(self, name: str, columns_names: List[str], columns_sql_types: List[str],
                 primary_key: Optional[str] = None, primary_key_sql_type: Optional[str] = None):
        """
        Initialise a Table instance

        :param name: name of the table
        :type name: str
        :param columns_names: names of the columns (except primary column)
        :type columns_names: List[str]
        :param columns_sql_types: sql types of the previous columns
        :type columns_sql_types: List[str]
        :param primary_key: name of the primary key (None, if no primary key is needed)
        :type primary_key: Optional[str]
        :param primary_key_sql_type: sql type of the primary key (None, if no primary key is needed)
        :type primary_key_sql_type: Optional[str]
        """
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
        'tradeId',
        'tdTime',
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
        'tranId',
        'dustTime',
        'asset',
        'assetAmount',
        'bnbAmount',
        'bnbFee',
    ],
    [
        'INTEGER',
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
        'interestTime',
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

LENDING_PURCHASE_TABLE = Table(
    'lending_purchase_history',
    [
        'purchaseTime',
        'lendingType',
        'asset',
        'amount'
    ],
    [
        'INTEGER',
        'TEXT',
        'TEXT',
        'INTEGER'
    ],
    primary_key='purchaseId',
    primary_key_sql_type='INTEGER'
)

LENDING_REDEMPTION_TABLE = Table(
    'lending_redemption_history',
    [
        'redemptionTime',
        'lendingType',
        'asset',
        'amount'
    ],
    [
        'INTEGER',
        'TEXT',
        'TEXT',
        'INTEGER'
    ]
)

CROSS_MARGIN_TRADE_TABLE = Table(
    'cross_margin_trade',
    [
        'tradeId',
        'tdTime',
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

CROSS_MARGIN_LOAN_TABLE = Table(
    "cross_margin_loan_table",
    [
        'loanTime',
        'asset',
        'principal',
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL'
    ],
    primary_key='txId',
    primary_key_sql_type='INTEGER'

)

CROSS_MARGIN_REPAY_TABLE = Table(
    "cross_margin_repay_table",
    [
        'repayTime',
        'asset',
        'principal',
        'interest',
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL',
        'REAL'
    ],
    primary_key='txId',
    primary_key_sql_type='INTEGER'

)

CROSS_MARGIN_INTEREST_TABLE = Table(
    "cross_margin_interest_table",
    [
        'interestTime',
        'asset',
        'interest',
        'interestType'
    ],
    [
        'INTEGER',
        'TEXT',
        'REAL',
        'TEXT'
    ]
)

UNIVERSAL_TRANSFER_TABLE = Table(
    "universal_transfer_table",
    [
        'trfType',
        'trfTime',
        'asset',
        'amount'
    ],
    [
        'TEXT',
        'INTEGER',
        'TEXT',
        'REAL'
    ],
    primary_key='tranId',
    primary_key_sql_type='INTEGER'
)
