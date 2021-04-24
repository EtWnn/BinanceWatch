import datetime
from typing import Optional

from BinanceWatch.storage.DataBase import DataBase, SQLConditionEnum
from BinanceWatch.storage import tables
from BinanceWatch.utils.time_utils import datetime_to_millistamp


class BinanceDataBase(DataBase):
    """
    Handles the recording of the binance account in a local database
    """

    def __init__(self, name: str = 'binance_db'):
        """
        Initialise a binance database instance

        :param name: name of the database
        :type name: str
        """
        super().__init__(name)

    def add_universal_transfer(self, transfer_id: int, transfer_type: str, transfer_time: int, asset: str,
                               amount: float, auto_commit: bool = True):
        """
        Add a universal transfer to the database

        :param transfer_id: id of the transfer
        :type transfer_id:  int
        :param transfer_type: enum of the transfer type (ex: 'MAIN_MARGIN')
        :type transfer_type: str
        :param transfer_time: millistamp of the operation
        :type transfer_time: int
        :param asset: asset that got transferred
        :type asset: str
        :param amount: amount transferred
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        table = tables.UNIVERSAL_TRANSFER_TABLE

        row = (transfer_id, transfer_type, transfer_time, asset, amount)
        self.add_row(table, row, auto_commit=auto_commit)

    def get_universal_transfers(self, transfer_type: Optional[str] = None, asset: Optional[str] = None,
                                start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return universal transfers stored in the database. Transfer type, Asset type and time filters can be used

        :param transfer_type: enum of the transfer type (ex: 'MAIN_MARGIN')
        :type transfer_type: Optional[str]
        :param asset: fetch only interests in this asset
        :type asset: Optional[str]
        :param start_time: fetch only interests after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only interests before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (1206491332,        # transfer id
                'MAIN_MARGIN',      # transfer type
                1589121841000,      # time
                'BNB',              # asset
                10.594112),         # amount
            ]
        """
        table = tables.UNIVERSAL_TRANSFER_TABLE

        conditions_list = []
        if transfer_type is not None:
            conditions_list.append((table.trfType,
                                    SQLConditionEnum.equal,
                                    transfer_type))
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.trfTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.trfTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_universal_transfer_time(self, transfer_type: str) -> int:
        """
        Return the latest time when a universal transfer was made
        If None, return the millistamp corresponding to 2017/01/01

        :param transfer_type: enum of the transfer type (ex: 'MAIN_MARGIN')
        :type transfer_type: str
        :return: millistamp
        :rtype: int
        """
        table = tables.UNIVERSAL_TRANSFER_TABLE
        conditions_list = [(table.trfType,
                            SQLConditionEnum.equal,
                            transfer_type)]
        selection = f"MAX({table.trfTime})"
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

    def add_isolated_transfer(self, transfer_id: int, transfer_type: str, transfer_time: int, isolated_symbol: str,
                              asset: str, amount: float, auto_commit: bool = True):
        """
        Add a universal transfer to the database

        :param transfer_id: id of the transfer
        :type transfer_id:  int
        :param transfer_type: enum of the transfer type (ex: 'MAIN_MARGIN')
        :type transfer_type: str
        :param transfer_time: millistamp of the operation
        :type transfer_time: int
        :param isolated_symbol: isolated symbol that received or sent the transfer
        :type isolated_symbol: str
        :param asset: asset that got transferred
        :type asset: str
        :param amount: amount transferred
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        table = tables.ISOLATED_MARGIN_TRANSFER_TABLE

        row = (transfer_id, transfer_type, transfer_time, isolated_symbol, asset, amount)
        self.add_row(table, row, auto_commit=auto_commit)

    def get_isolated_transfers(self, isolated_symbol: Optional[str] = None, start_time: Optional[int] = None,
                               end_time: Optional[int] = None):
        """
        Return isolated transfers stored in the database. isolated_symbol and time filters can be used

        :param isolated_symbol: for isolated margin, provided the trading symbol otherwise it will be counted a cross
            margin data
        :type isolated_symbol: Optional[str]
        :param start_time: fetch only transfers after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only transfers before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (1206491332,        # transfer id
                'IN',               # transfer type (IN or OUT)
                1589121841000,      # time
                'BTCBUSD',          # isolated symbol
                'BTC',              # asset
                10.594112),         # amount
            ]
        """
        table = tables.ISOLATED_MARGIN_TRANSFER_TABLE

        conditions_list = []
        if isolated_symbol is not None:
            conditions_list.append((table.symbol,
                                    SQLConditionEnum.equal,
                                    isolated_symbol))
        if start_time is not None:
            conditions_list.append((table.trfTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.trfTime,
                                    SQLConditionEnum.lower,
                                    end_time))

        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_isolated_transfer_time(self, isolated_symbol: str) -> int:
        """
        Return the latest time when a isolated margin transfer was made
        If None, return the millistamp corresponding to 2017/01/01

        :param isolated_symbol: isolated symbol that received or sent the transfers
        :type isolated_symbol: str
        :return: millistamp
        :rtype: int
        """
        table = tables.ISOLATED_MARGIN_TRANSFER_TABLE
        conditions_list = [(table.symbol,
                            SQLConditionEnum.equal,
                            isolated_symbol)]
        selection = f"MAX({table.trfTime})"
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

    def add_margin_interest(self, interest_time: int, asset: str, interest: float, interest_type: str,
                            isolated_symbol: Optional[str] = None, auto_commit: bool = True):
        """
        Add a margin interest to the database

        :param interest_time: millistamp of the operation
        :type interest_time: int
        :param asset: asset that got repaid
        :type asset: str
        :param interest: amount of interest accrued
        :type interest: float
        :param interest_type: one of (PERIODIC, ON_BORROW, PERIODIC_CONVERTED, ON_BORROW_CONVERTED)
        :type interest_type: str
        :param isolated_symbol: for isolated margin, provided the trading symbol otherwise it will be counted a cross
            margin data
        :type isolated_symbol: Optional[str]
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_INTEREST_TABLE
            row = (interest_time, asset, interest, interest_type)
        else:
            table = tables.ISOLATED_MARGIN_INTEREST_TABLE
            row = (interest_time, isolated_symbol, asset, interest, interest_type)

        self.add_row(table, row, auto_commit=auto_commit)

    def get_margin_interests(self, margin_type: str, asset: Optional[str] = None, isolated_symbol: Optional[str] = None,
                             start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return margin interests stored in the database. Asset type and time filters can be used

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param asset: fetch only interests in this asset
        :type asset: Optional[str]
        :param isolated_symbol: only for isolated margin, provide the trading symbol (otherwise cross data are returned)
        :type isolated_symbol: Optional[str]
        :param start_time: fetch only interests after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only interests before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            # cross margin
            [
                1559415215400,             # time
                'BNB',                     # asset
                0.51561,                   # interest
                'PERIODIC_CONVERTED'),     # interest type
            ]

            # isolated margin
            [
                1559415215400,             # time
                'BTCBUSD',                 # symbol
                'BUSD',                    # asset
                0.51561,                   # interest
                'PERIODIC'),               # interest type
            ]
        """
        conditions_list = []

        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_INTEREST_TABLE
        elif margin_type == 'isolated':
            table = tables.ISOLATED_MARGIN_INTEREST_TABLE
            if isolated_symbol is not None:
                conditions_list.append((table.isolated_symbol,
                                        SQLConditionEnum.equal,
                                        isolated_symbol))
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

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

    def get_last_margin_interest_time(self, asset: Optional[str] = None, isolated_symbol: Optional[str] = None) -> int:
        """
        Return the latest time when a margin interest was accured on a defined asset or on all assets
        If None, return the millistamp corresponding to 2017/01/01

        :param asset: name of the asset charged as interest
        :type asset: Optional[str]
        :param isolated_symbol: only for isolated margin, provide the trading symbol (otherwise cross data are returned)
        :type isolated_symbol: Optional[str]
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_INTEREST_TABLE
        else:
            table = tables.ISOLATED_MARGIN_INTEREST_TABLE
            conditions_list.append((table.symbol,
                                    SQLConditionEnum.equal,
                                    isolated_symbol))

        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))

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

    def add_repay(self, tx_id: int, repay_time: int, asset: str, principal: float,
                  interest: float, isolated_symbol: Optional[str] = None, auto_commit: bool = True):
        """
        Add a repay to the database

        :param tx_id: binance id for the transaction (uniqueness?)
        :type tx_id: int
        :param repay_time: millitstamp of the operation
        :type repay_time: int
        :param asset: asset that got repaid
        :type asset: str
        :param principal: principal amount repaid for the loan
        :type principal: float
        :param interest: amount of interest repaid for the loan
        :type interest: float
        :param isolated_symbol: for isolated margin, provided the trading symbol otherwise it will be counted a cross
            margin data
        :type isolated_symbol: Optional[str]
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_REPAY_TABLE
            row = (tx_id, repay_time, asset, principal, interest)
        else:
            table = tables.ISOLATED_MARGIN_REPAY_TABLE
            row = (tx_id, repay_time, isolated_symbol, asset, principal, interest)

        self.add_row(table, row, auto_commit=auto_commit)

    def get_repays(self, margin_type: str, asset: Optional[str] = None, isolated_symbol: Optional[str] = None,
                   start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return repays stored in the database. Asset type and time filters can be used

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type: str
        :param asset: fetch only repays of this asset
        :type asset: Optional[str]
        :param isolated_symbol: only for isolated margin, provide the trading symbol
        :type isolated_symbol: Optional[str]
        :param start_time: fetch only repays after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only repays before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            # cross margin
            [
                (8289451654,        # transaction id
                1559415215400,      # time
                'USDT',             # asset
                145.5491462,        # principal
                0.51561),           # interest
            ]

            # isolated margin
            [
                (8289451654,        # transaction id
                1559415215400,      # time
                'BTCUSDT',          # isolated symbol
                'USDT',             # asset
                145.5491462,        # principal
                0.51561),           # interest
            ]
        """
        conditions_list = []

        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_REPAY_TABLE
        elif margin_type == 'isolated':
            table = tables.ISOLATED_MARGIN_REPAY_TABLE
            if isolated_symbol is not None:
                conditions_list.append((table.isolated_symbol,
                                        SQLConditionEnum.equal,
                                        isolated_symbol))
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

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

    def get_last_repay_time(self, asset: str, isolated_symbol: Optional[str] = None) -> int:
        """
        Return the latest time when a repay was made on a defined asset
        If None, return the millistamp corresponding to 2017/01/01

        :param asset: name of the asset repaid
        :type asset: str
        :param isolated_symbol: only for isolated margin, provide the trading symbol (otherwise cross data are returned)
        :type isolated_symbol: Optional[str]
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_REPAY_TABLE
        else:
            table = tables.ISOLATED_MARGIN_REPAY_TABLE
            conditions_list.append((table.symbol,
                                    SQLConditionEnum.equal,
                                    isolated_symbol))

        conditions_list.append((table.asset,
                                SQLConditionEnum.equal,
                                asset))

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

    def add_loan(self, tx_id: int, loan_time: int, asset: str, principal: float,
                 isolated_symbol: Optional[str] = None, auto_commit: bool = True):
        """
        Add a loan to the database

        :param tx_id: binance id for the transaction (uniqueness?)
        :type tx_id: int
        :param loan_time: millitstamp of the operation
        :type loan_time: int
        :param asset: asset that got loaned
        :type asset: str
        :param principal: amount of loaned asset
        :type principal: float
        :param isolated_symbol: for isolated margin, provided the trading symbol otherwise it will be counted a cross
            margin data
        :type isolated_symbol: Optional[str]
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_LOAN_TABLE
            row = (tx_id, loan_time, asset, principal)
        else:
            row = (tx_id, loan_time, isolated_symbol, asset, principal)
            table = tables.ISOLATED_MARGIN_LOAN_TABLE

        self.add_row(table, row, auto_commit=auto_commit)

    def get_loans(self, margin_type: str, asset: Optional[str] = None, isolated_symbol: Optional[str] = None,
                  start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return loans stored in the database. Asset type and time filters can be used

        :param margin_type: either 'cross' or 'isolated'
        :type margin_type:
        :param asset: fetch only loans of this asset
        :type asset: Optional[str]
        :param isolated_symbol: only for isolated margin, provide the trading symbol
        :type isolated_symbol: Optional[str]
        :param start_time: fetch only loans after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only loans before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            # cross margin
            [
                (8289451654,        # transaction id
                1559415215400,      # time
                'USDT',             # asset
                145.5491462),       # amount
            ]

            # isolated margin
            [
                (8289451654,        # transaction id
                1559415215400,      # time
                'BTCUSDT',          # symbol
                'USDT',             # asset
                145.5491462),       # amount
            ]


        """
        conditions_list = []
        if margin_type == 'cross':
            table = tables.CROSS_MARGIN_LOAN_TABLE
        elif margin_type == 'isolated':
            table = tables.ISOLATED_MARGIN_LOAN_TABLE
            if isolated_symbol is not None:
                conditions_list.append((table.isolated_symbol,
                                        SQLConditionEnum.equal,
                                        isolated_symbol))
        else:
            raise ValueError(f"margin type should be 'cross' or 'isolated' but {margin_type} was received")

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

    def get_last_loan_time(self, asset: str, isolated_symbol: Optional[str] = None) -> int:
        """
        Return the latest time when an loan was made on a defined asset
        If None, return the millistamp corresponding to 2017/01/01

        :param asset: name of the asset loaned
        :type asset: str
        :param isolated_symbol: only for isolated margin, provide the trading symbol (otherwise cross data are returned)
        :type isolated_symbol: Optional[str]
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        if isolated_symbol is None:
            table = tables.CROSS_MARGIN_LOAN_TABLE
        else:
            table = tables.ISOLATED_MARGIN_LOAN_TABLE
            conditions_list.append((table.symbol,
                                    SQLConditionEnum.equal,
                                    isolated_symbol))

        conditions_list.append((table.asset,
                                SQLConditionEnum.equal,
                                asset))
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

    def add_lending_redemption(self, redemption_time: int, lending_type: str, asset: str, amount: float,
                               auto_commit: bool = True):
        """
        Add a lending redemption to the database

        :param redemption_time: millitstamp of the operation
        :type redemption_time: int
        :param lending_type: either 'DAILY', 'ACTIVITY' or 'CUSTOMIZED_FIXED'
        :type lending_type: str
        :param asset: asset lent
        :type asset: str
        :param amount: amount of asset redeemed
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (redemption_time, lending_type, asset, amount)
        self.add_row(tables.LENDING_REDEMPTION_TABLE, row, auto_commit=auto_commit)

    def get_lending_redemptions(self, lending_type: Optional[str] = None, asset: Optional[str] = None,
                                start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return lending redemptions stored in the database. Asset type and time filters can be used

        :param lending_type: fetch only redemptions from this lending type
        :type lending_type: Optional[str]
        :param asset: fetch only redemptions from this asset
        :type asset: Optional[str]
        :param start_time: fetch only redemptions after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only redemptions before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                1612841562000,      # time
                'DAILY',            # lending type
                'LTC',              # asset
                1.89151684),        # amount
            ]
        """
        conditions_list = []
        table = tables.LENDING_REDEMPTION_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.redemptionTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.redemptionTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_lending_redemption_time(self, lending_type: Optional[str] = None) -> int:
        """
        Return the latest time when an lending redemption was made.
        If None, return the millistamp corresponding to 2017/01/01

        :param lending_type: type of lending
        :type lending_type: str
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        table = tables.LENDING_REDEMPTION_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        selection = f"MAX({table.redemptionTime})"
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

    def add_lending_purchase(self, purchase_id: int, purchase_time: int, lending_type: str, asset: str, amount: float,
                             auto_commit: bool = True):
        """
        Add a lending purchase to the database

        :param purchase_id: id of the purchase
        :type purchase_id: int
        :param purchase_time: millitstamp of the operation
        :type purchase_time: int
        :param lending_type: either 'DAILY', 'ACTIVITY' or 'CUSTOMIZED_FIXED'
        :type lending_type: str
        :param asset: asset lent
        :type asset: str
        :param amount: amount of asset lent
        :type amount: float
        :param auto_commit: if the database should commit the change made, default True
        :type auto_commit: bool
        :return: None
        :rtype: None
        """
        row = (purchase_id, purchase_time, lending_type, asset, amount)
        self.add_row(tables.LENDING_PURCHASE_TABLE, row, auto_commit=auto_commit)

    def get_lending_purchases(self, lending_type: Optional[str] = None, asset: Optional[str] = None,
                              start_time: Optional[int] = None, end_time: Optional[int] = None):
        """
        Return lending purchases stored in the database. Asset type and time filters can be used

        :param lending_type: fetch only purchases from this lending type
        :type lending_type: Optional[str]
        :param asset: fetch only purchases from this asset
        :type asset: Optional[str]
        :param start_time: fetch only purchases after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only purchases before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (58516828,          # purchase id
                1612841562000,      # time
                'DAILY',            # lending type
                'LTC',              # asset
                1.89151684),        # amount
            ]
        """
        conditions_list = []
        table = tables.LENDING_PURCHASE_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        if asset is not None:
            conditions_list.append((table.asset,
                                    SQLConditionEnum.equal,
                                    asset))
        if start_time is not None:
            conditions_list.append((table.purchaseTime,
                                    SQLConditionEnum.greater_equal,
                                    start_time))
        if end_time is not None:
            conditions_list.append((table.purchaseTime,
                                    SQLConditionEnum.lower,
                                    end_time))
        return self.get_conditions_rows(table, conditions_list=conditions_list)

    def get_last_lending_purchase_time(self, lending_type: Optional[str] = None) -> int:
        """
        Return the latest time when an lending purchase was made.
        If None, return the millistamp corresponding to 2017/01/01

        :param lending_type: type of lending
        :type lending_type: str
        :return: millistamp
        :rtype: int
        """
        conditions_list = []
        table = tables.LENDING_PURCHASE_TABLE
        if lending_type is not None:
            conditions_list.append((table.lendingType,
                                    SQLConditionEnum.equal,
                                    lending_type))
        selection = f"MAX({table.purchaseTime})"
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
        Add an lending interest to the database

        :param time: millitstamp of the operation
        :type time: int
        :param lending_type: either 'DAILY', 'ACTIVITY' or 'CUSTOMIZED_FIXED'
        :type lending_type: str
        :param asset: asset that was received
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
        Return lending interests stored in the database. Asset type and time filters can be used

        :param lending_type: fetch only interests from this lending type
        :type lending_type: Optional[str]
        :param asset: fetch only interests from this asset
        :type asset: Optional[str]
        :param start_time: fetch only interests after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only interests before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (1619846515000,     # time
                'DAILY',            # lending type
                'DOT',              # asset
                0.00490156)         # amount
            ]

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

    def get_last_lending_interest_time(self, lending_type: Optional[str] = None) -> int:
        """
        Return the latest time when an interest was received.
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

    def add_spot_dust(self, tran_id: str, time: int, asset: str, asset_amount: float, bnb_amount: float, bnb_fee: float,
                      auto_commit: bool = True):
        """
        Add dust operation to the database

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
        Return dusts stored in the database. Asset type and time filters can be used

        :param asset: fetch only dusts from this asset
        :type asset: Optional[str]
        :param start_time: fetch only dusts after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only dusts before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (82156485284,       # transaction id
                1605489113400,      # time
                'TRX',              # asset
                102.78415879,       # asset amount
                0.09084498,         # bnb amount
                0.00171514),        # bnb fee
            ]
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
        Add a dividend to the database

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
        Return dividends stored in the database. Asset type and time filters can be used

        :param asset: fetch only dividends of this asset
        :type asset: Optional[str]
        :param start_time: fetch only dividends after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only dividends before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                (8945138941,         # dividend id
                1594513589000,       # time
                'TRX',               # asset
                0.18745654),         # amount
            ]

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
        Fetch the latest time a dividend has been distributed on the spot account. If None is found,
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
        Add a withdraw to the database

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
        Return withdraws stored in the database. Asset type and time filters can be used

        :param asset: fetch only withdraws of this asset
        :type asset: Optional[str]
        :param start_time: fetch only withdraws after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only withdraws before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                ('84984dcqq5z11gyjfa',  # withdraw id
                'aazd8949vredqs56dz',   # transaction id
                1599138389000,          # withdraw time
                'XTZ',                  # asset
                57.0194,                # amount
                0.5),                   # fee
            ]

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
        Fetch the latest time a withdraw has been made on the spot account. If None is found, return the millistamp
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
        Add a deposit to the database

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
        Return deposits stored in the database. Asset type and time filters can be used

        :param asset: fetch only deposits of this asset
        :type asset: Optional[str]
        :param start_time: fetch only deposits after this millistamp
        :type start_time: Optional[int]
        :param end_time: fetch only deposits before this millistamp
        :type end_time: Optional[int]
        :return: The raw rows selected as saved in the database
        :rtype: List[Tuple]

        .. code-block:: python

            [
                ('azdf5e6a1d5z',    # transaction id
                1589479004000,      # deposit time
                'LTC',              # asset
                14.25),             # amount
            ]

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
        Fetch the latest time a deposit has been made on the spot account. If None is found, return the millistamp
        corresponding to 2017/1/1

        :return: last deposit millistamp
        :rtype: int
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
                  price: float, fee: float, fee_asset: str, is_buyer: bool, symbol: Optional[str] = None,
                  auto_commit: bool = True):
        """
        Add a trade to the database

        :param trade_type: type trade executed
        :type trade_type: string, must be one of {'spot', 'cross_margin', 'isolated_margin'}
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
        :param fee_asset: token unit for the fee
        :type fee_asset: str
        :param is_buyer: if the trade is a buy or a sell
        :type is_buyer: bool
        :param symbol: trading symbol, mandatory if thr trade_type is isolated margin
        :type symbol: Optional[str]
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
        elif trade_type == 'isolated_margin':
            table = tables.ISOLATED_MARGIN_TRADE_TABLE
            if symbol is None:
                raise ValueError("trade_type was isolated margin but symbol was not provided")
            row = (trade_id, trade_time, symbol, asset, ref_asset, qty, price, fee, fee_asset, int(is_buyer))
        else:
            msg = f"trade type should be one of ('spot', 'cross_margin', 'isolated_margin') but {trade_type} was" \
                  f" received"
            raise ValueError(msg)
        self.add_row(table, row, auto_commit)

    def get_trades(self, trade_type: str, start_time: Optional[int] = None, end_time: Optional[int] = None,
                   asset: Optional[str] = None, ref_asset: Optional[str] = None):
        """
        Return trades stored in the database. asset type, ref_asset type and time filters can be used

        :param trade_type: type trade executed
        :type trade_type: string, must be one of ('spot', 'cross_margin', 'isolated_margin')
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

        Return for spot and cross margin:

        .. code-block:: python

            [
                (384518832,         # trade_id
                1582892988052,      # trade time
                'BTC',              # asset
                'USDT',             # ref asset
                0.0015,             # asset quantity
                9011.2,             # asset price to ref asset
                0.01425,            # fee
                'USDT',             # fee asset
                0),                 # is_buyer
            ]

        Return for isolated margin:

        .. code-block:: python

            [
                (384518832,         # trade_id
                1582892988052,      # trade time
                'BTCUSDT',          # symbol
                'BTC',              # asset
                'USDT',             # ref asset
                0.0015,             # asset quantity
                9011.2,             # asset price to ref asset
                0.01425,            # fee
                'USDT',             # fee asset
                0),                 # is_buyer
            ]


        """
        if trade_type == 'spot':
            table = tables.SPOT_TRADE_TABLE
        elif trade_type == 'cross_margin':
            table = tables.CROSS_MARGIN_TRADE_TABLE
        elif trade_type == 'isolated_margin':
            table = tables.ISOLATED_MARGIN_TRADE_TABLE
        else:
            msg = f"trade type should be one of ('spot', 'cross_margin', 'isolated_margin') but {trade_type} was" \
                  f" received"
            raise ValueError(msg)
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
        Return the latest trade id for a trading pair. If none is found, return -1

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
        elif trade_type == 'isolated_margin':
            table = tables.ISOLATED_MARGIN_TRADE_TABLE
        else:
            msg = f"trade type should be one of ('spot', 'cross_margin', 'isolated_margin') but {trade_type} was" \
                  f" received"
            raise ValueError(msg)

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
