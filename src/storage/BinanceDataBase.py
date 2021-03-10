import datetime
from typing import Optional

from src.storage.DataBase import DataBase, SQLConditionEnum
from src.storage import tables
from src.utils.time_utils import datetime_to_millistamp


class BinanceDataBase(DataBase):
    """
    Handles the recording of the binance account in a local database
    """

    def __init__(self, name: str = 'binance_db'):
        super().__init__(name)

    def add_dust(self, dust_id: int, time: int, asset: str, asset_amount: float, bnb_amount: float, bnb_fee: float,
                 auto_commit: bool = True):
        """
        add dust operation to the database

        :param dust_id: id of the operation
        :type dust_id: int
        :param time: millitstamp of the operation
        :type time: int
        :param asset: asset that got converted to BNB
        :type asset: str
        :param asset_amount: amount of asset that got converted
        :type asset_amount: float
        :param bnb_amount: amount received from the conversion
        :type bnb_amount: float
        :param bnb_fee: fee amount in BNB
        :type bnb_fee: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """

        row = (dust_id, time, asset, asset_amount, bnb_amount, bnb_fee)
        self.add_row(tables.SPOT_DUST_TABLE, row, auto_commit=auto_commit)

    def get_spot_dusts(self, asset: Optional[str] = None, start_time: Optional[int] = None,
                       end_time: Optional[int] = None):
        """
        return dusts stored in the database. Asset type and time filters can be used

        :param asset: fetch only dusts from this asset
        :type asset: Optional[str]
        :param start_time: fetch only dusts after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only dusts before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        if asset is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[2],
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[1],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[1],
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(tables.SPOT_DUST_TABLE, conditions_list=conditions_list)

    def add_dividend(self, div_id: int, div_time: int, asset: str, amount: float, auto_commit: bool = True):
        """
        add a dividend to the database

        :param div_id: dividend id
        :type div_id: int
        :param div_time: millistamp of dividend reception
        :type div_time: int
        :param asset: name of the dividend unit
        :type asset: str
        :param amount: amount of asset distributed
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (div_id, div_time, asset, amount)
        self.add_row(tables.SPOT_DIVIDEND_TABLE, row, auto_commit=auto_commit)

    def get_spot_dividends(self, asset: Optional[str] = None, start_time: Optional[int] = None,
                           end_time: Optional[int] = None):
        """
        return dividends stored in the database. Asset type and time filters can be used

        :param asset: fetch only dividends of this asset
        :type asset: Optional[str]
        :param start_time: fetch only dividends after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only dividends before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        if asset is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[2],
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[1],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.SPOT_DIVIDEND_TABLE.columns_names[1],
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(tables.SPOT_DIVIDEND_TABLE, conditions_list=conditions_list)

    def get_last_spot_dividend_time(self) -> int:
        """
        fetch the latest time a dividend has been distributed on the spot account. If None is found,
        return the millistamp corresponding to 2017/1/1

        :return:
        """
        selection = f"MAX({tables.SPOT_DIVIDEND_TABLE.columns_names[1]})"
        result = self.get_conditions_rows(tables.SPOT_WITHDRAW_TABLE,
                                          selection=selection)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_withdraw(self, withdraw_id: str, tx_id: str, apply_time: int, asset: str, amount: float, fee: float,
                     auto_commit: bool = True):
        """
        add a withdraw to the database

        :param withdraw_id: binance if of the withdraw
        :type withdraw_id: str
        :param tx_id: transaction id
        :type tx_id: str
        :param apply_time: millistamp when the withdraw was requested
        :type apply_time: int
        :param asset: name of the token
        :type asset: str
        :param amount: amount of token withdrawn
        :type amount: float
        :param fee: amount of the asset paid for the withdraw
        :type fee: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (withdraw_id, tx_id, apply_time, asset, amount, fee)
        self.add_row(tables.SPOT_WITHDRAW_TABLE, row, auto_commit=auto_commit)

    def get_spot_withdraws(self, asset: Optional[str] = None, start_time: Optional[int] = None,
                           end_time: Optional[int] = None):
        """
        return withdraws stored in the database. Asset type and time filters can be used

        :param asset: fetch only withdraws of this asset
        :type asset: Optional[str]
        :param start_time: fetch only withdraws after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only withdraws before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        if asset is not None:
            conditions_list.append((tables.SPOT_WITHDRAW_TABLE.columns_names[3],
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((tables.SPOT_WITHDRAW_TABLE.columns_names[2],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.SPOT_WITHDRAW_TABLE.columns_names[2],
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(tables.SPOT_WITHDRAW_TABLE, conditions_list=conditions_list)

    def get_last_spot_withdraw_time(self) -> int:
        """
        fetch the latest time a withdraw has been made on the spot account. If None is found, return the millistamp
        corresponding to 2017/1/1

        :return:
        """
        selection = f"MAX({tables.SPOT_WITHDRAW_TABLE.columns_names[2]})"
        result = self.get_conditions_rows(tables.SPOT_WITHDRAW_TABLE,
                                          selection=selection)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_deposit(self, tx_id: str, insert_time: int, amount: float, asset: str, auto_commit=True):
        """
        add a deposit to the database

        :param tx_id: transaction id
        :type tx_id: str
        :param insert_time: millistamp when the deposit arrived on binance
        :type insert_time: int
        :param amount: amount of token deposited
        :type amount: float
        :param asset: name of the token
        :type asset: str
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (tx_id, insert_time, asset, amount)
        self.add_row(tables.SPOT_DEPOSIT_TABLE, row, auto_commit)

    def get_spot_deposits(self, asset: Optional[str] = None, start_time: Optional[int] = None,
                          end_time: Optional[int] = None):
        """
        return deposits stored in the database. Asset type and time filters can be used

        :param asset: fetch only deposits of this asset
        :type asset: Optional[str]
        :param start_time: fetch only deposits after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only deposits before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        if asset is not None:
            conditions_list.append((tables.SPOT_DEPOSIT_TABLE.columns_names[2],
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((tables.SPOT_DEPOSIT_TABLE.columns_names[1],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.SPOT_DEPOSIT_TABLE.columns_names[1],
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(tables.SPOT_DEPOSIT_TABLE, conditions_list=conditions_list)

    def get_last_spot_deposit_time(self) -> int:
        """
        fetch the latest time a deposit has been made on the spot account. If None is found, return the millistamp
        corresponding to 2017/1/1

        :return:
        """
        selection = f"MAX({tables.SPOT_DEPOSIT_TABLE.columns_names[1]})"
        result = self.get_conditions_rows(tables.SPOT_DEPOSIT_TABLE,
                                          selection=selection)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_spot_trade(self, trade_id: int, millistamp: int, asset: str, ref_asset: str, qty: float, price: float,
                       fee: float, fee_asset: str, is_buyer: bool, auto_commit=True):
        """
        add a trade to the database

        :param trade_id: id of the trade (binance id, unique per trading pair)
        :type trade_id: int
        :param millistamp: millistamp of the trade
        :type millistamp: int
        :param asset: name of the asset in the trading pair (ex 'BTC' for 'BTCUSDT')
        :type asset: string
        :param ref_asset: name of the reference asset in the trading pair (ex 'USDT' for 'BTCUSDT')
        :type ref_asset: string
        :param qty: quantity of asset exchanged
        :type qty: float
        :param price: price of the asset regarding the ref_asset
        :type price: float
        :param fee: amount kept by the exchange
        :type fee: float
        :param fee_asset:token unit for the fee
        :type fee_asset: str
        :param is_buyer: if the trade is a buy or a sell
        :type is_buyer: bool
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        key = f'{asset}{ref_asset}{trade_id}'
        row = (key, trade_id, millistamp, asset, ref_asset, qty, price, fee, fee_asset, int(is_buyer))
        self.add_row(tables.SPOT_TRADE_TABLE, row, auto_commit)

    def get_spot_trades(self, start_time: Optional[int] = None, end_time: Optional[int] = None,
                        asset: Optional[str] = None, ref_asset: Optional[str] = None):
        """
        return trades stored in the database. asset type, ref_asset type and time filters can be used

        :param start_time: fetch only trades after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only trades before this millistamp
        :type end_time: Optional[int]
        :param asset: fetch only trades with this asset
        :type asset: Optional[str]
        :param ref_asset:  fetch only trades with this ref_asset
        :type ref_asset: Optional[str]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        if start_time is not None:
            conditions_list.append((tables.SPOT_TRADE_TABLE.columns_names[2],
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((tables.SPOT_TRADE_TABLE.columns_names[2],
                                    SQLConditionEnum.lower,
                                    end_time))
        if asset is not None:
            conditions_list.append((tables.SPOT_TRADE_TABLE.columns_names[3],
                                    SQLConditionEnum.equal,
                                    asset))
        if ref_asset is not None:
            conditions_list.append((tables.SPOT_TRADE_TABLE.columns_names[4],
                                    SQLConditionEnum.equal,
                                    ref_asset))
        return self.get_conditions_rows(tables.SPOT_TRADE_TABLE, conditions_list=conditions_list)

    def get_max_trade_id(self, asset: str, ref_asset: str) -> int:
        """
        return the latest trade id for a trading pair. If none is found, return -1

        :param asset: name of the asset in the trading pair (ex 'BTC' for 'BTCUSDT')
        :type asset: string
        :param ref_asset: name of the reference asset in the trading pair (ex 'USDT' for 'BTCUSDT')
        :type ref_asset: string
        :return: latest trade id
        :rtype: int
        """
        selection = f"MAX({tables.SPOT_TRADE_TABLE.columns_names[1]})"
        conditions_list = [
            (tables.SPOT_TRADE_TABLE.columns_names[3],
             SQLConditionEnum.equal,
             asset),
            (tables.SPOT_TRADE_TABLE.columns_names[4],
             SQLConditionEnum.equal,
             ref_asset)
        ]
        result = self.get_conditions_rows(tables.SPOT_TRADE_TABLE,
                                          selection=selection,
                                          conditions_list=conditions_list)
        try:
            result = result[0][0]
        except IndexError:
            return -1
        if result is None:
            return -1
        return result
