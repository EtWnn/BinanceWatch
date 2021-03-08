from dataclasses import dataclass
from typing import List


@dataclass
class Table:
    name: str
    columns_names: List[str]
    columns_sql_types: List[str]


SpotTradeTable = Table(
    'spot_trade',
    [

    ],
    [

    ]
)
