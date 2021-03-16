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

    def add_repay(self, margin_type: str, tx_id: int, repay_time: int, asset: str, principal: float,
                  interest: float, auto_commit: bool = True):
        """
        add a repay to the database

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param tx_id: binance id for the transaction (uniqueness?)
        :type tx_id: int
        :param repay_time: millitstamp of the operation
        :type repay_time: int
        :param asset: asset that got repaid
        :type asset: str
        :param principal: principal amount repaid for the loan
        :type principal: float
        :param interest: amount of interest repaid for the loan
        :type interest:
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_REPAY_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        row = (tx_id, repay_time, asset, principal, interest)
        self.add_row(table, row, auto_commit=auto_commit)

    def get_repays(self, margin_type: str, asset: Optional[str] = None, start_time: Optional[int] = None,
                   end_time: Optional[int] = None):
        """
        return repays stored in the database. Asset type and time filters can be used

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param asset: fetch only repays of this asset
        :type asset: Optional[str]
        :param start_time: fetch only repays after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only repays before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_REPAY_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        conditions_list = []
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.repayTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.repayTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_repay_time(self, asset: str, margin_type: str):
        """
        return the latest time when a repay was made on a defined asset
        If None, return the millistamp corresponding to 2017/01/01

        :param asset: name of the asset repaid
        :type asset: str
        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :return: millistamp
        :rtype: int
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_REPAY_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        conditions_list = [(table.asset,
                            SQLConditionEnum.equal,
                            asset)]
        selection = f"MAX({table.repayTime})"
        result = self.get_conditions_rows(table,
                                          selection=selection,
                                          conditions_list=conditions_list)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_loan(self, margin_type: str, tx_id: int, loan_time: int, asset: str, principal: float,
                 auto_commit: bool = True):
        """
        add a loan to the database

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param tx_id: binance id for the transaction (uniqueness?)
        :type tx_id: int
        :param loan_time: millitstamp of the operation
        :type loan_time: int
        :param asset: asset that got loaned
        :type asset: str
        :param principal: amount of loaned asset
        :type principal: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_LOAN_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        row = (tx_id, loan_time, asset, principal)
        self.add_row(table, row, auto_commit=auto_commit)

    def get_loans(self, margin_type: str, asset: Optional[str] = None, start_time: Optional[int] = None,
                  end_time: Optional[int] = None):
        """
        return loans stored in the database. Asset type and time filters can be used

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param asset: fetch only loans of this asset
        :type asset: Optional[str]
        :param start_time: fetch only loans after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only loans before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_LOAN_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        conditions_list = []
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.loanTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.loanTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_loan_time(self, asset: str, margin_type: str):
        """
        return the latest time when an loan was made on a defined asset
        If None, return the millistamp corresponding to 2017/01/01

        :param asset: name of the asset loaned
        :type asset: str
        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :return: millistamp
        :rtype: int
        """
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_LOAN_TABLE
        elif margin_type == 'isolated':
            raise NotImplementedError
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

        conditions_list = [(table.asset,
                            SQLConditionEnum.equal,
                            asset)]
        selection = f"MAX({table.loanTime})"
        result = self.get_conditions_rows(table,
                                          selection=selection,
                                          conditions_list=conditions_list)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_lending_interest(self, time: int, lending_type: str, asset: str, amount: float,
                             auto_commit: bool = True):
        """
        add an lending interest to the database

        :param time: millitstamp of the operation
        :type time: int
        :param lending_type: either 'DAILY', 'ACTIVITY' or 'CUSTOMIZED_FIXED'
        :type lending_type: str
        :param asset: asset that got converted to BNB
        :type asset: str
        :param amount: amount of asset received
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (time, lending_type, asset, amount)
        self.add_row(tables.LENDING_INTEREST_TABLE, row, auto_commit=auto_commit)

    def get_lending_interests(self, lending_type: Optional[str] = None, asset: Optional[str] = None,
                              start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        return lending interests stored in the database. Asset type and time filters can be used

        :param lending_type:fetch only interests from this lending type
        :type lending_type: Optional[str]
        :param asset: fetch only interests from this asset
        :type asset: Optional[str]
        :param start_time: fetch only interests after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only interests before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]
        """
        conditions_list = []
        table = tables.LENDING_INTEREST_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.interestTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.interestTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_lending_interest_time(self, lending_type: Optional[str] = None):
        """
        return the latest time when an interest was received.
        If None, return the millistamp corresponding to 2017/01/01

        :param lending_type: type of lending
        :type lending_type: str
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        table = tables.LENDING_INTEREST_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        selection = f"MAX({table.interestTime})"
        result = self.get_conditions_rows(table,
                                          selection=selection,
                                          conditions_list=conditions_list)
        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_dust(self, tran_id: str, time: int, asset: str, asset_amount: float, bnb_amount: float, bnb_fee: float,
                 auto_commit: bool = True):
        """
        add dust operation to the database
        https://binance-docs.github.io/apidocs/spot/en/#dustlog-user_data

        :param tran_id: id of the transaction (non unique)
        :type tran_id: str
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

        row = (tran_id, time, asset, asset_amount, bnb_amount, bnb_fee)
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
        table = tables.SPOT_DUST_TABLE
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.dustTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.dustTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

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
        table = tables.SPOT_DIVIDEND_TABLE
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.divTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.divTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_spot_dividend_time(self) -> int:
        """
        fetch the latest time a dividend has been distributed on the spot account. If None is found,
        return the millistamp corresponding to 2017/1/1

        :return:
        """
        table = tables.SPOT_DIVIDEND_TABLE
        selection = f"MAX({table.divTime})"
        result = self.get_conditions_rows(table,
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
        table = tables.SPOT_WITHDRAW_TABLE
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.applyTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.applyTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_spot_withdraw_time(self) -> int:
        """
        fetch the latest time a withdraw has been made on the spot account. If None is found, return the millistamp
        corresponding to 2017/1/1

        :return:
        """
        table = tables.SPOT_WITHDRAW_TABLE
        selection = f"MAX({table.applyTime})"
        result = self.get_conditions_rows(table,
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
        table = tables.SPOT_DEPOSIT_TABLE
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.insertTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.insertTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_spot_deposit_time(self) -> int:
        """
        fetch the latest time a deposit has been made on the spot account. If None is found, return the millistamp
        corresponding to 2017/1/1

        :return:
        """
        table = tables.SPOT_DEPOSIT_TABLE
        selection = f"MAX({table.insertTime})"
        result = self.get_conditions_rows(table,
                                          selection=selection)

        default = datetime_to_millistamp(datetime.datetime(2017, 1, 1, tzinfo=datetime.timezone.utc))
        try:
            result = result[0][0]
        except IndexError:
            return default
        if result is None:
            return default
        return result

    def add_trade(self, trade_type: str, trade_id: int, trade_time: int, asset: str, ref_asset: str, qty: float,
                  price: float, fee: float, fee_asset: str, is_buyer: bool, auto_commit=True):
        """
        add a trade to the database

        :param trade_type: type trade executed
        :type trade_type: string, must be one of {'spot', 'cross_margin'}
        :param trade_id: id of the trade (binance id, unique per trading pair)
        :type trade_id: int
        :param trade_time: millistamp of the trade
        :type trade_time: int
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
        row = (trade_id, trade_time, asset, ref_asset, qty, price, fee, fee_asset, int(is_buyer))
        if trade_type == 'spot':
            table = tables.SPOT_TRADE_TABLE
        elif trade_type == 'cross_margin':
            table = tables.CROSS_MARGIN_TRADE_TABLE
        else:
            raise ValueError(f"trade type should be one of ('spot', 'cross_margin') but {trade_type} was received")
        self.add_row(table, row, auto_commit)

    def get_trades(self, trade_type: str, start_time: Optional[int] = None, end_time: Optional[int] = None,
                   asset: Optional[str] = None, ref_asset: Optional[str] = None):
        """
        return trades stored in the database. asset type, ref_asset type and time filters can be used

        :param trade_type: type trade executed
        :type trade_type: string, must be one of ('spot', 'cross_margin')
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
        if trade_type == 'spot':
            table = tables.SPOT_TRADE_TABLE
        elif trade_type == 'cross_margin':
            table = tables.CROSS_MARGIN_TRADE_TABLE
        else:
            raise ValueError(f"trade type should be one of ('spot', 'cross_margin') but {trade_type} was received")
        conditions_list = []
        if start_time is not None:
            conditions_list.append((table.tdTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.tdTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if ref_asset is not None:
            conditions_list.append((table.refAsset,
                                    SQLConditionEnum.equal,
                                    ref_asset))
        return self.get_conditions_rows(table, conditions_list=conditions_list, order_list=[table.tdTime])

    def get_max_trade_id(self, asset: str, ref_asset: str, trade_type: str) -> int:
        """
        return the latest trade id for a trading pair. If none is found, return -1

        :param asset: name of the asset in the trading pair (ex 'BTC' for 'BTCUSDT')
        :type asset: string
        :param ref_asset: name of the reference asset in the trading pair (ex 'USDT' for 'BTCUSDT')
        :type ref_asset: string
        :param trade_type: type trade executed
        :type trade_type: string, must be one of {'spot', 'cross_margin'}
        :return: latest trade id
        :rtype: int
        """
        if trade_type == 'spot':
            table = tables.SPOT_TRADE_TABLE
        elif trade_type == 'cross_margin':
            table = tables.CROSS_MARGIN_TRADE_TABLE
        else:
            raise ValueError(f"trade type should be one of {'spot', 'cross_margin'} but {trade_type} was received")

        selection = f"MAX({table.tradeId})"
        conditions_list = [
            (table.asset,
             SQLConditionEnum.equal,
             asset),
            (table.refAsset,
             SQLConditionEnum.equal,
             ref_asset)
        ]
        result = self.get_conditions_rows(table, selection=selection, conditions_list=conditions_list)
        try:
            result = result[0][0]
        except IndexError:
            return -1
        if result is None:
            return -1
        return result
