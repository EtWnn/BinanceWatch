import datetime
import math
import time
from typing import Optional, Dict

import dateparser
from binance.client import Client
from binance.exceptions import BinanceAPIException
from tqdm import tqdm

from BinanceWatch.storage import tables
from BinanceWatch.utils.LoggerGenerator import LoggerGenerator
from BinanceWatch.utils.time_utils import datetime_to_millistamp
from BinanceWatch.storage.BinanceDataBase import BinanceDataBase


class BinanceManager:
    """
    This class is in charge of filling the database by calling the binance API
    """
    API_MAX_RETRY = 3

    def __init__(self, api_key: str, api_secret: str, account_name: str = 'default'):
        """
        initialise the binance manager.

        :param api_key: key for the Binance api
        :type api_key: str
        :param api_secret: secret for the Binance api
        :type api_secret: str
        :param account_name: if you have several accounts to monitor, you need to give them different names or the
        database will collide
        :type account_name: str
        """
        self.account_name = account_name
        self.db = BinanceDataBase(name=f"{self.account_name}_db")
        self.client = Client(api_key=api_key, api_secret=api_secret)
        self.logger = LoggerGenerator.get_logger(f"BinanceManager_{self.account_name}")

    def update_spot(self):
        """
        call all update methods related to the spot account

        :return: None
        :rtype: None
        """
        self.update_all_spot_trades()
        self.update_spot_deposits()
        self.update_spot_withdraws()
        self.update_spot_dusts()
        self.update_spot_dividends()
        self.update_universal_transfers(transfer_filter='MAIN')

    def update_cross_margin(self):
        """
        call all update methods related to cross margin spot account

        :return: None
        :rtype: None
        """
        self.update_all_cross_margin_trades()
        self.update_cross_margin_loans()
        self.update_cross_margin_interests()
        self.update_cross_margin_repays()
        self.update_universal_transfers(transfer_filter='MARGIN')

    def update_lending(self):
        """
        call all update methods related to lending activities

        :return: None
        :rtype: None
        """
        self.update_lending_interests()
        self.update_lending_purchases()
        self.update_lending_redemptions()

    def update_universal_transfers(self, transfer_filter: Optional[str] = None):
        """
        update the universal transfers database.

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.query_universal_transfer_history
        https://binance-docs.github.io/apidocs/spot/en/#query-user-universal-transfer-history

        :param transfer_filter: if not None, only the transfers containing this filter will be updated (ex: 'MAIN')
        :type transfer_filter: Optional[str]
        :return: None
        :rtype: None
        """
        all_types = ['MAIN_C2C', 'MAIN_UMFUTURE', 'MAIN_CMFUTURE', 'MAIN_MARGIN', 'MAIN_MINING', 'C2C_MAIN',
                     'C2C_UMFUTURE', 'C2C_MINING', 'C2C_MARGIN', 'UMFUTURE_MAIN', 'UMFUTURE_C2C',
                     'UMFUTURE_MARGIN', 'CMFUTURE_MAIN', 'CMFUTURE_MARGIN', 'MARGIN_MAIN', 'MARGIN_UMFUTURE',
                     'MARGIN_CMFUTURE', 'MARGIN_MINING', 'MARGIN_C2C', 'MINING_MAIN', 'MINING_UMFUTURE',
                     'MINING_C2C', 'MINING_MARGIN']
        if transfer_filter is not None:
            transfers_types = list(filter(lambda x: transfer_filter in x, all_types))
        else:
            transfers_types = all_types
        pbar = tqdm(total=len(transfers_types))
        for transfer_type in transfers_types:
            pbar.set_description(f"fetching transfer type {transfer_type}")
            latest_time = self.db.get_last_universal_transfer_time(transfer_type=transfer_type) + 1
            current = 1
            while True:
                universal_transfers = self.client.query_universal_transfer_history(type=transfer_type,
                                                                                   startTime=latest_time,
                                                                                   current=current,
                                                                                   size=100)
                try:
                    universal_transfers = universal_transfers['rows']
                except KeyError:
                    break
                for transfer in universal_transfers:
                    self.db.add_universal_transfer(transfer_id=transfer['tranId'],
                                                   transfer_type=transfer['type'],
                                                   transfer_time=transfer['timestamp'],
                                                   asset=transfer['asset'],
                                                   amount=float(transfer['amount'])
                                                   )

                if len(universal_transfers):
                    current += 1  # next page
                    self.db.commit()
                else:
                    break
            pbar.update()
        pbar.close()

    def update_cross_margin_interests(self):
        """
        update the interests for all cross margin assets

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-repay-record-user_data

        :return:
        :rtype:
        """
        margin_type = 'cross'
        latest_time = self.db.get_last_margin_interest_time(margin_type)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        pbar = tqdm()
        pbar.set_description("fetching cross margin interests")
        while True:
            params = {
                'current': current,
                'startTime': latest_time + 1000,
                'size': 100,
                'archived': archived
            }
            # no built-in method yet in python-binance for margin/interestHistory
            interests = self.client._request_margin_api('get', 'margin/interestHistory', signed=True, data=params)

            for interest in interests['rows']:
                self.db.add_margin_interest(margin_type=margin_type,
                                            interest_time=interest['interestAccuredTime'],
                                            asset=interest['asset'],
                                            interest=interest['interest'],
                                            interest_type=interest['type'],
                                            auto_commit=False)

            if len(interests['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived interests
                current = 1
                archived = False
                latest_time = self.db.get_last_margin_interest_time(margin_type)
            else:
                break
            pbar.update()
        pbar.close()

    def update_cross_margin_repays(self):
        """
        update the repays for all cross margin assets

        :return: None
        :rtype: None
        """
        symbols_info = self.client._request_margin_api('get', 'margin/allPairs', data={})  # not built-in yet
        assets = set()
        for symbol_info in symbols_info:
            assets.add(symbol_info['base'])
            assets.add(symbol_info['quote'])

        pbar = tqdm(total=len(assets))
        for asset in assets:
            pbar.set_description(f"fetching {asset} cross margin repays")
            self.update_margin_asset_repay(asset=asset)
            pbar.update()
        pbar.close()

    def update_margin_asset_repay(self, asset: str, isolated_symbol=''):
        """
        update the repays database for a specified asset.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-repay-record-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_repay_details

        :param asset: asset for the repays
        :type asset: str
        :param isolated_symbol: the symbol must be specified of isolated margin, otherwise cross margin data is returned
        :type isolated_symbol: str
        :return: None
        :rtype: None
        """
        margin_type = 'cross' if isolated_symbol == '' else 'isolated'
        latest_time = self.db.get_last_repay_time(asset=asset, margin_type=margin_type)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        while True:
            repays = self.client.get_margin_repay_details(asset=asset,
                                                          current=current,
                                                          startTime=latest_time + 1000,
                                                          archived=archived,
                                                          isolatedSymbol=isolated_symbol,
                                                          size=100)
            for repay in repays['rows']:
                if repay['status'] == 'CONFIRMED':
                    self.db.add_repay(margin_type=margin_type,
                                      tx_id=repay['txId'],
                                      repay_time=repay['timestamp'],
                                      asset=repay['asset'],
                                      principal=repay['principal'],
                                      interest=repay['interest'],
                                      auto_commit=False)

            if len(repays['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived repays
                current = 1
                archived = False
                latest_time = self.db.get_last_repay_time(asset=asset, margin_type=margin_type)
            else:
                break

    def update_cross_margin_loans(self):
        """
        update the loans for all cross margin assets

        :return: None
        :rtype: None
        """
        symbols_info = self.client._request_margin_api('get', 'margin/allPairs', data={})  # not built-in yet
        assets = set()
        for symbol_info in symbols_info:
            assets.add(symbol_info['base'])
            assets.add(symbol_info['quote'])

        pbar = tqdm(total=len(assets))
        for asset in assets:
            pbar.set_description(f"fetching {asset} cross margin loans")
            self.update_margin_asset_loans(asset=asset)
            pbar.update()
        pbar.close()

    def update_margin_asset_loans(self, asset: str, isolated_symbol=''):
        """
        update the loans database for a specified asset.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-loan-record-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_loan_details

        :param asset: asset for the loans
        :type asset: str
        :param isolated_symbol: the symbol must be specified of isolated margin, otherwise cross margin data is returned
        :type isolated_symbol: str
        :return: None
        :rtype: None
        """
        margin_type = 'cross' if isolated_symbol == '' else 'isolated'
        latest_time = self.db.get_last_loan_time(asset=asset, margin_type=margin_type)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        while True:
            loans = self.client.get_margin_loan_details(asset=asset,
                                                        current=current,
                                                        startTime=latest_time + 1000,
                                                        archived=archived,
                                                        isolatedSymbol=isolated_symbol,
                                                        size=100)
            for loan in loans['rows']:
                if loan['status'] == 'CONFIRMED':
                    self.db.add_loan(margin_type=margin_type,
                                     tx_id=loan['txId'],
                                     loan_time=loan['timestamp'],
                                     asset=loan['asset'],
                                     principal=loan['principal'],
                                     auto_commit=False)

            if len(loans['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived loans
                current = 1
                archived = False
                latest_time = self.db.get_last_loan_time(asset=asset, margin_type=margin_type)
            else:
                break

    def update_cross_margin_symbol_trades(self, asset: str, ref_asset: str, limit: int = 1000):
        """
        This update the cross_margin trades in the database for a single trading pair.
        It will check the last trade id and will requests the all trades after this trade_id.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-trade-list-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_trades

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
        last_trade_id = self.db.get_max_trade_id(asset, ref_asset, 'cross_margin')
        while True:
            new_trades = self.client.get_margin_trades(symbol=symbol, fromId=last_trade_id + 1, limit=limit)
            for trade in new_trades:
                self.db.add_trade(trade_type='cross_margin',
                                  trade_id=int(trade['id']),
                                  trade_time=int(trade['time']),
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

    def update_all_cross_margin_trades(self, limit: int = 1000):
        """
        This update the cross margin trades in the database for every trading pairs

        :param limit: max size of each trade requests
        :type limit: int
        :return: None
        :rtype: None
        """
        symbols_info = self.client._request_margin_api('get', 'margin/allPairs', data={})  # not built-in yet
        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            pbar.set_description(f"fetching {symbol_info['symbol']} cross margin trades")
            self.update_cross_margin_symbol_trades(asset=symbol_info['base'],
                                                   ref_asset=symbol_info['quote'],
                                                   limit=limit)
            pbar.update()
        pbar.close()

    def update_lending_redemptions(self):
        """
        update the lending redemptions database.

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_lending_redemption_history
        https://binance-docs.github.io/apidocs/spot/en/#get-redemption-record-user_data

        :return: None
        :rtype: None
        """
        lending_types = ['DAILY', 'ACTIVITY', 'CUSTOMIZED_FIXED']
        pbar = tqdm(total=3)
        for lending_type in lending_types:
            pbar.set_description(f"fetching lending redemptions of type {lending_type}")
            latest_time = self.db.get_last_lending_redemption_time(lending_type=lending_type) + 1
            current = 1
            while True:
                lending_redemptions = self.client.get_lending_redemption_history(lendingType=lending_type,
                                                                                 startTime=latest_time,
                                                                                 current=current,
                                                                                 size=100)
                for li in lending_redemptions:
                    if li['status'] == 'PAID':
                        self.db.add_lending_redemption(redemption_time=li['createTime'],
                                                       lending_type=lending_type,
                                                       asset=li['asset'],
                                                       amount=li['amount']
                                                       )

                if len(lending_redemptions):
                    current += 1  # next page
                    self.db.commit()
                else:
                    break
            pbar.update()
        pbar.close()

    def update_lending_purchases(self):
        """
        update the lending purchases database.

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_lending_purchase_history
        https://binance-docs.github.io/apidocs/spot/en/#get-purchase-record-user_data

        :return: None
        :rtype: None
        """
        lending_types = ['DAILY', 'ACTIVITY', 'CUSTOMIZED_FIXED']
        pbar = tqdm(total=3)
        for lending_type in lending_types:
            pbar.set_description(f"fetching lending purchases of type {lending_type}")
            latest_time = self.db.get_last_lending_purchase_time(lending_type=lending_type) + 1
            current = 1
            while True:
                lending_purchases = self.client.get_lending_purchase_history(lendingType=lending_type,
                                                                             startTime=latest_time,
                                                                             current=current,
                                                                             size=100)
                for li in lending_purchases:
                    if li['status'] == 'SUCCESS':
                        self.db.add_lending_purchase(purchase_id=li['purchaseId'],
                                                     purchase_time=li['createTime'],
                                                     lending_type=li['lendingType'],
                                                     asset=li['asset'],
                                                     amount=li['amount']
                                                     )

                if len(lending_purchases):
                    current += 1  # next page
                    self.db.commit()
                else:
                    break
            pbar.update()
        pbar.close()

    def update_lending_interests(self):
        """
        update the lending interests database.

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_lending_interest_history
        https://binance-docs.github.io/apidocs/spot/en/#get-interest-history-user_data-2

        :return: None
        :rtype: None
        """
        lending_types = ['DAILY', 'ACTIVITY', 'CUSTOMIZED_FIXED']
        pbar = tqdm(total=3)
        for lending_type in lending_types:
            pbar.set_description(f"fetching lending interests of type {lending_type}")
            latest_time = self.db.get_last_lending_interest_time(lending_type=lending_type) + 3600 * 1000  # add 1 hour
            current = 1
            while True:
                lending_interests = self.client.get_lending_interest_history(lendingType=lending_type,
                                                                             startTime=latest_time,
                                                                             current=current,
                                                                             size=100)
                for li in lending_interests:
                    self.db.add_lending_interest(time=li['time'],
                                                 lending_type=li['lendingType'],
                                                 asset=li['asset'],
                                                 amount=li['interest']
                                                 )

                if len(lending_interests):
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

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_dust_log
        https://binance-docs.github.io/apidocs/spot/en/#dustlog-user_data

        :return: None
        :rtype: None
        """
        self.db.drop_table(tables.SPOT_DUST_TABLE)

        result = self.client.get_dust_log()
        dusts = result['results']
        pbar = tqdm(total=dusts['total'])
        pbar.set_description("fetching spot dusts")
        for d in dusts['rows']:
            for sub_dust in d['logs']:
                date_time = dateparser.parse(sub_dust['operateTime'] + 'Z')
                self.db.add_dust(tran_id=sub_dust['tranId'],
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
        """
        update the dividends database (earnings distributed by Binance)
        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_asset_dividend_history
        https://binance-docs.github.io/apidocs/spot/en/#asset-dividend-record-user_data

        :param day_jump: length of the time window in days, max is 90
        :type day_jump: float
        :param limit: max number of dividends to retrieve per call, max is 500
        :type limit: int
        :return: None
        :rtype: None
        """
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

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_withdraw_history
        https://binance-docs.github.io/apidocs/spot/en/#withdraw-history-user_data

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

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_deposit_history
        https://binance-docs.github.io/apidocs/spot/en/#deposit-history-user_data

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

        sources:
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_my_trades
        https://binance-docs.github.io/apidocs/spot/en/#account-trade-list-user_data

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
        last_trade_id = self.db.get_max_trade_id(asset, ref_asset, 'spot')
        while True:
            new_trades = self.client.get_my_trades(symbol=symbol, fromId=last_trade_id + 1, limit=limit)
            for trade in new_trades:
                self.db.add_trade(trade_type='spot',
                                  trade_id=int(trade['id']),
                                  trade_time=int(trade['time']),
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
            pbar.set_description(f"fetching {symbol_info['symbol']} spot trades")
            self.update_spot_symbol_trades(asset=symbol_info['baseAsset'],
                                           ref_asset=symbol_info['quoteAsset'],
                                           limit=limit)
            pbar.update()
        pbar.close()

    def _call_binance_client(self, method_name: str, params: Optional[Dict] = None, retry_count: int = 0):
        """
        This method is used to handle rate limits: if a rate limits is breached, it will wait the necessary time
        to call again the API.

        :param method_name: name of the method binance.Client to call
        :type method_name: str
        :param params: parameters to pass to the above method
        :type params: Dict
        :param retry_count: internal use only to count the number of retry if rate limits are breached
        :type retry_count: int
        :return: response of binance.Client method
        :rtype: Dict
        """
        if params is None:
            params = dict()
        if retry_count >= BinanceManager.API_MAX_RETRY:
            raise RuntimeError(f"The API rate limits has been breached {retry_count} times")

        try:
            return getattr(self.client, method_name)(**params)
        except BinanceAPIException as err:
            if err.code == -1021:  # API rate Limits
                wait_time = err.response.headers['Retry-After']
                if err.response.status_code == 418:  # ban
                    self.logger.error(f"API calls resulted in a ban, retry in {wait_time} seconds")
                    raise err
                self.logger.info(f"API calls resulted in a breach of rate limits, will retry after {wait_time} seconds")
                time.sleep(wait_time + 1)
                return self._call_binance_client(method_name, params, retry_count + 1)
            raise err
