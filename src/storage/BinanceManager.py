import datetime
import math

import dateparser
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

    def update_lending_interests(self):
        """
        update the lending interests database.
        for each update

        :return: None
        :rtype: None
        """
        lending_types = ['DAILY', 'ACTIVITY', 'CUSTOMIZED_FIXED']
        pbar = tqdm(total=3)
        for lending_type in lending_types:
            pbar.set_description(f"fetching lending type {lending_type}")
            latest_time = self.db.get_last_lending_interest_time(lending_type=lending_type) + 3600 * 1000  # add 1 hour
            current = 1
            while True:
                lending_interests = self.client.get_lending_interest_history(lendingType=lending_type,
                                                                             startTime=latest_time,
                                                                             current=current,
                                                                             limit=100)
                for li in lending_interests:
                    print(li)
                    self.db.add_lending_interest(int_id=str(li['time']) + li['asset'] + li['lendingType'],
                                                 time=li['time'],
                                                 lending_type=li['lendingType'],
                                                 asset=li['asset'],
                                                 amount=li['interest']
                                                 )

                if lending_interests:
                    current += 1  # next page
                    self.db.commit()
                else:
                    break
            pbar.update()
        pbar.close()

    def update_spot_dusts(self):
        """
        update the dust database. As there is no way to get the dust by id or timeframe, the table is cleared
        for each update

        :return: None
        :rtype: None
        """
        self.drop_dust_table()

        result = self.client.get_dust_log()
        dusts = result['results']
        pbar = tqdm(total=dusts['total'])
        pbar.set_description("fetching dusts")
        for d in dusts['rows']:
            for sub_dust in d['logs']:
                date_time = dateparser.parse(sub_dust['operateTime'] + 'Z')
                self.db.add_dust(dust_id=str(sub_dust['tranId']) + sub_dust['fromAsset'],
                                 time=datetime_to_millistamp(date_time),
                                 asset=sub_dust['fromAsset'],
                                 asset_amount=sub_dust['amount'],
                                 bnb_amount=sub_dust['transferedAmount'],
                                 bnb_fee=sub_dust['serviceChargeAmount'],
                                 auto_commit=False
                                 )
            pbar.update()
        self.db.commit()
        pbar.close()

    def update_spot_dividends(self, day_jump: float = 90, limit: int = 500):
        limit = min(500, limit)
        delta_jump = min(day_jump, 90) * 24 * 3600 * 1000
        start_time = self.db.get_last_spot_dividend_time() + 1
        now_millistamp = datetime_to_millistamp(datetime.datetime.now(tz=datetime.timezone.utc))
        pbar = tqdm(total=math.ceil((now_millistamp - start_time) / delta_jump))
        pbar.set_description("fetching spot dividends")
        while start_time < now_millistamp:
            params = {
                'startTime': start_time,
                'endTime': start_time + delta_jump,
                'limit': limit
            }
            # the stable working version of client.get_asset_dividend_history is not released yet,
            # for now it has a post error, so this protected member is used in the meantime
            result = self.client._request_margin_api('get',
                                                     'asset/assetDividend',
                                                     True,
                                                     data=params
                                                     )
            dividends = result['rows']
            for div in dividends:
                self.db.add_dividend(div_id=int(div['tranId']),
                                     div_time=int(div['divTime']),
                                     asset=div['asset'],
                                     amount=float(div['amount']),
                                     auto_commit=False
                                     )
            pbar.update()
            if len(dividends) < limit:
                start_time += delta_jump
            else:  # limit was reached before the end of the time windows
                start_time = int(dividends[0]['divTime']) + 1
            if len(dividends):
                self.db.commit()
        pbar.close()

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
        pbar = tqdm(total=math.ceil((now_millistamp - start_time) / delta_jump))
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
        pbar = tqdm(total=math.ceil((now_millistamp - start_time) / delta_jump))
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

    def drop_spot_dividends_table(self):
        """
        erase the spot dividends table

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_DIVIDEND_TABLE)

    def drop_dust_table(self):
        """
        erase the spot dust table

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_DUST_TABLE)

    def drop_all_tables(self):
        """
        erase all the tables of the database

        :return: None
        :rtype: None
        """
        self.drop_spot_deposit_table()
        self.drop_spot_trade_table()
        self.drop_spot_withdraw_table()
        self.drop_spot_dividends_table()
        self.drop_dust_table()
