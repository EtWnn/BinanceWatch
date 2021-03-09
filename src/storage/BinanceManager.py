import datetime
import math

from binance.client import Client
from tqdm import tqdm

from src.utils.time_utils import datetime_to_millistamp
from src.storage.BinanceDataBase import BinanceDataBase
from src.utils.credentials import CredentialManager
from src.storage import tables


class BinanceManager:
    """
    This class is in charge of filling the database by calling the binance API
    """

    def __init__(self):
        self.db = BinanceDataBase()
        credentials = CredentialManager.get_api_credentials("Binance")
        self.client = Client(**credentials)

    def update_spot_withdraws(self, day_jump: float = 90):
        """
        This fetch the crypto withdraws made on the spot account from the last withdraw time in the database to now.
        It is done with multiple call, each having a time window of day_jump days.
        The withdraws are then saved in the database.
        Only successful withdraws are fetched.

        :param day_jump: length of the time window for each call (max 90)
        :type day_jump: float
        :return: None
        :rtype: None
        """
        delta_jump = min(day_jump, 90) * 24 * 3600 * 1000
        start_time = self.db.get_last_spot_withdraw_time() + 1
        now_millistamp = datetime_to_millistamp(datetime.datetime.now(tz=datetime.timezone.utc))
        pbar = tqdm(total=math.ceil((now_millistamp - start_time)/delta_jump))
        pbar.set_description("fetching spot withdraws")
        while start_time < now_millistamp:
            result = self.client.get_withdraw_history(startTime=start_time, endTime=start_time + delta_jump, status=6)
            withdraws = result['withdrawList']
            for withdraw in withdraws:
                self.db.add_withdraw(withdraw_id=withdraw['id'],
                                     tx_id=withdraw['txId'],
                                     apply_time=int(withdraw['applyTime']),
                                     asset=withdraw['asset'],
                                     amount=float(withdraw['amount']),
                                     fee=float(withdraw['transactionFee']),
                                     auto_commit=False
                                     )
            pbar.update()
            start_time += delta_jump
            if len(withdraws):
                self.db.commit()
        pbar.close()

    def update_spot_deposits(self, day_jump: float = 90):
        """
        This fetch the crypto deposit made on the spot account from the last deposit time in the database to now.
        It is done with multiple call, each having a time window of day_jump days.
        The deposits are then saved in the database.
        Only successful deposits are fetched.

        :param day_jump: length of the time window for each call (max 90)
        :type day_jump: float
        :return: None
        :rtype: None
        """
        delta_jump = min(day_jump, 90) * 24 * 3600 * 1000
        start_time = self.db.get_last_spot_deposit_time() + 1
        now_millistamp = datetime_to_millistamp(datetime.datetime.now(tz=datetime.timezone.utc))
        pbar = tqdm(total=math.ceil((now_millistamp - start_time)/delta_jump))
        pbar.set_description("fetching spot deposits")
        while start_time < now_millistamp:
            result = self.client.get_deposit_history(startTime=start_time, endTime=start_time + delta_jump, status=1)
            deposits = result['depositList']
            for deposit in deposits:
                self.db.add_deposit(tx_id=deposit['txId'],
                                    asset=deposit['asset'],
                                    insert_time=int(deposit['insertTime']),
                                    amount=float(deposit['amount']),
                                    auto_commit=False)
            pbar.update()
            start_time += delta_jump
            if len(deposits):
                self.db.commit()
        pbar.close()

    def update_spot_symbol_trades(self, asset: str, ref_asset: str, limit: int = 1000):
        """
        This update the spot trades in the database for a single trading pair. It will check the last trade id and will
        requests the all trades after this trade_id.

        :param asset: name of the asset in the trading pair (ex 'BTC' for 'BTCUSDT')
        :type asset: string
        :param ref_asset: name of the reference asset in the trading pair (ex 'USDT' for 'BTCUSDT')
        :type ref_asset: string
        :param limit: max size of each trade requests
        :type limit: int
        :return: None
        :rtype: None
        """
        limit = min(1000, limit)
        symbol = asset + ref_asset
        last_trade_id = self.db.get_max_trade_id(asset, ref_asset)
        while True:
            new_trades = self.client.get_my_trades(symbol=symbol, fromId=last_trade_id + 1, limit=limit)
            for trade in new_trades:
                self.db.add_spot_trade(trade_id=int(trade['id']),
                                       millistamp=int(trade['time']),
                                       asset=asset,
                                       ref_asset=ref_asset,
                                       qty=float(trade['qty']),
                                       price=float(trade['price']),
                                       fee=float(trade['commission']),
                                       fee_asset=trade['commissionAsset'],
                                       is_buyer=trade['isBuyer'],
                                       auto_commit=False
                                       )
                last_trade_id = max(last_trade_id, int(trade['id']))
            if len(new_trades):
                self.db.commit()
            if len(new_trades) < limit:
                break

    def update_all_spot_trades(self, limit: int = 1000):
        """
        This update the spot trades in the database for every trading pairs

        :param limit: max size of each trade requests
        :type limit: int
        :return: None
        :rtype: None
        """
        symbols_info = self.client.get_exchange_info()['symbols']
        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            pbar.set_description(f"fetching {symbol_info['symbol']}")
            self.update_spot_symbol_trades(asset=symbol_info['baseAsset'],
                                           ref_asset=symbol_info['quoteAsset'],
                                           limit=limit)
            pbar.update()
        pbar.close()

    def drop_spot_trade_table(self):
        """
        erase the spot trades table

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_TRADE_TABLE)

    def drop_spot_deposit_table(self):
        """
        erase the spot deposits table

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_DEPOSIT_TABLE)

    def drop_spot_withdraw_table(self):
        """
        erase the spot withdraws table

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_WITHDRAW_TABLE)

    def drop_all_tables(self):
        """
        erase all the tables of the database

        :return: None
        :rtype: None
        """
        self.drop_spot_deposit_table()
        self.drop_spot_trade_table()
        self.drop_spot_withdraw_table()
