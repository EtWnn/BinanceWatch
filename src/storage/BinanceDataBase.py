from typing import Optional

from src.storage.DataBase import DataBase, SQLConditionEnum
from src.storage import tables


class BinanceDataBase(DataBase):

    def __init__(self, name: str = 'binance_db'):
        super().__init__(name)

    def add_spot_trade(self, trade_id: int, millistamp: int, asset: str, ref_asset: str, qty: float, price: float,
                       fee: float, fee_asset: str, is_buyer: bool, auto_commit=True):
        key = f'{asset}{ref_asset}{trade_id}'
        row = (key, trade_id, millistamp, asset, ref_asset, qty, price, fee, fee_asset, int(is_buyer))
        self.add_row(tables.BINANCE_SPOT_TRADE_TABLE, row, auto_commit)

    def get_spot_trades(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
                        asset: Optional[str] = None, ref_asset: Optional[str] = None):
        conditions_list = []
        if start_time is not None:
            conditions_list.append((tables.BINANCE_SPOT_TRADE_TABLE.columns_names[2],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.BINANCE_SPOT_TRADE_TABLE.columns_names[2],
                                    SQLConditionEnum.lower,
                                    end_time))
        if asset is not None:
            conditions_list.append((tables.BINANCE_SPOT_TRADE_TABLE.columns_names[3],
                                    SQLConditionEnum.equal,
                                    asset))
        if ref_asset is not None:
            conditions_list.append((tables.BINANCE_SPOT_TRADE_TABLE.columns_names[4],
                                    SQLConditionEnum.equal,
                                    ref_asset))
        return self.get_conditions_rows(tables.BINANCE_SPOT_TRADE_TABLE, conditions_list=conditions_list)

    def get_max_trade_id(self, asset: str, ref_asset: str):
        selection = f"MAX({tables.BINANCE_SPOT_TRADE_TABLE.columns_names[1]})"
        conditions_list = [
            (tables.BINANCE_SPOT_TRADE_TABLE.columns_names[3],
             SQLConditionEnum.equal,
             asset),
            (tables.BINANCE_SPOT_TRADE_TABLE.columns_names[4],
             SQLConditionEnum.equal,
             ref_asset)
        ]
        result = self.get_conditions_rows(tables.BINANCE_SPOT_TRADE_TABLE,
                                          selection=selection,
                                          conditions_list=conditions_list)
        try:
            result = result[0][0]
        except IndexError:
            return -1
        if result is None:
            return -1
        return result
