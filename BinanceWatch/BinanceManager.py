import datetime
import math
import time
from typing import Optional, Dict, List, Union

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
        Initialise the binance manager.

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
        call all update methods related to cross margin account

        :return: None
        :rtype: None
        """
        self.update_all_cross_margin_trades()
        self.update_cross_margin_loans()
        self.update_margin_interests()
        self.update_cross_margin_repays()
        self.update_universal_transfers(transfer_filter='MARGIN')

    def update_isolated_margin(self):
        """
        call all update methods related to isolated margin account

        :return: None
        :rtype: None
        """
        self.update_isolated_margin_transfers()  # fetch transfers across all isolated symbols

        # we will now update only the isolated symbols that have been funded
        transfers = self.db.get_isolated_transfers()
        symbols_info = []
        for _, _, _, symbol, token, _ in transfers:
            if symbol.startswith(token):
                asset, ref_asset = token, symbol[len(token):]
            else:
                asset, ref_asset = symbol[:-len(token)], token
            symbols_info.append({'asset': asset, 'ref_asset': ref_asset, 'symbol': symbol})

        self.update_isolated_margin_trades(symbols_info)
        self.update_isolated_margin_loans(symbols_info)
        self.update_isolated_margin_interests(symbols_info)
        self.update_isolated_margin_repays(symbols_info)


    def update_lending(self):
        """
        call all update methods related to lending activities

        :return: None
        :rtype: None
        """
        self.update_lending_interests()
        self.update_lending_purchases()
        self.update_lending_redemptions()

    def get_margin_symbol_info(self, isolated: bool) -> List[Dict]:
        """
        Return information about margin symbols as provided by the binance API


        sources:
        https://binance-docs.github.io/apidocs/spot/en/#get-all-isolated-margin-symbol-user_data
        https://binance-docs.github.io/apidocs/spot/en/#get-all-cross-margin-pairs-market_data

        :param isolated: If isolated data are to be returned, otherwise it will be cross margin data
        :type isolated: bool
        :return: Info on the trading symbols
        :rtype: List[Dict]

        .. code-block:: python

            # cross margin
            [
                {
                    'id': 351637150141315861,
                    'symbol': 'BNBBTC',
                    'base': 'BNB',
                    'quote': 'BTC',
                    'isMarginTrade': True,
                    'isBuyAllowed': True,
                    'isSellAllowed': True
                },
                ...
            ]

            # isolated margin
            [
                {
                    'symbol': '1INCHBTC',
                    'base': '1INCH',
                    'quote': 'BTC',
                    'isMarginTrade': True,
                    'isBuyAllowed': True,
                    'isSellAllowed': True
                },
                ...
            ]

        """
        client_params = {
            'method': 'get',
            'data': {}
        }
        if isolated:
            client_params['path'] = 'margin/isolated/allPairs'
            client_params['signed'] = True
        else:
            client_params['path'] = 'margin/allPairs'
        return self._call_binance_client('_request_margin_api', client_params)

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
                client_params = {
                    'type': transfer_type,
                    'startTime': latest_time,
                    'current': current,
                    'size': 100
                }
                universal_transfers = self._call_binance_client('query_universal_transfer_history', client_params)

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

    def update_isolated_margin_transfers(self, symbols_info: Optional[List[Dict]] = None):
        """
        Update the transfers to and from isolated symbols

        :param symbols_info: details on the symbols to fetch repays on. Each dictionary needs the fields 'asset' and
            'ref_asset'. If not provided, will update all isolated symbols.
        :type symbols_info: Optional[List[Dict]]
        :return: None
        :rtype: None
        """
        asset_key = 'asset'
        ref_asset_key = 'ref_asset'
        if symbols_info is None:
            symbols_info = self.get_margin_symbol_info(isolated=True)
            asset_key = 'base'
            ref_asset_key = 'quote'

        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            asset = symbol_info[asset_key]
            ref_asset = symbol_info[ref_asset_key]
            symbol = symbol_info.get('symbol', f"{asset}{ref_asset}")

            pbar.set_description(f"fetching isolated margin transfers for {symbol}")
            self.update_isolated_symbol_transfers(isolated_symbol=symbol)
            pbar.update()

        pbar.close()

    def update_isolated_symbol_transfers(self, isolated_symbol: str):
        """
        Update the transfers made to and from an isolated margin symbol

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#get-isolated-margin-transfer-history-user_data

        :param isolated_symbol: isolated margin symbol of trading
        :type isolated_symbol: str
        :return:
        :rtype:
        """
        latest_time = self.db.get_last_isolated_transfer_time(isolated_symbol=isolated_symbol)
        current = 1

        while True:
            params = {
                'symbol': isolated_symbol,
                'current': current,
                'startTime': latest_time + 1,
                'size': 100,
            }

            # no built-in method yet in python-binance for margin/interestHistory
            client_params = {
                'method': 'get',
                'path': 'margin/isolated/transfer',
                'signed': True,
                'data': params
            }
            transfers = self._call_binance_client('_request_margin_api', client_params)

            for transfer in transfers['rows']:
                if (transfer['transFrom'], transfer['transTo']) == ('SPOT', 'ISOLATED_MARGIN'):
                    transfer_type = 'IN'
                elif (transfer['transFrom'], transfer['transTo']) == ('SPOT', 'ISOLATED_MARGIN'):
                    transfer_type = 'OUT'
                else:
                    raise ValueError(f"unrecognised transfer: {transfer['transFrom']} -> {transfer['transTo']}")

                self.db.add_isolated_transfer(transfer_id=transfer['txId'],
                                              transfer_type=transfer_type,
                                              transfer_time=transfer['timestamp'],
                                              isolated_symbol=isolated_symbol,
                                              asset=transfer['asset'],
                                              amount=transfer['amount'],
                                              auto_commit=False)

            if len(transfers['rows']):
                current += 1  # next page
                self.db.commit()
            else:
                break

    def update_isolated_margin_interests(self, symbols_info: Optional[List[Dict]] = None):
        """
        Update the interests for isolated margin assets

        :param symbols_info: details on the symbols to fetch repays on. Each dictionary needs the fields 'asset' and
            'ref_asset'. If not provided, will update all isolated symbols.
        :type symbols_info: Optional[List[Dict]]
        :return: None
        :rtype: None
        """
        asset_key = 'asset'
        ref_asset_key = 'ref_asset'
        if symbols_info is None:
            symbols_info = self.get_margin_symbol_info(isolated=True)
            asset_key = 'base'
            ref_asset_key = 'quote'

        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            asset = symbol_info[asset_key]
            ref_asset = symbol_info[ref_asset_key]
            symbol = symbol_info.get('symbol', f"{asset}{ref_asset}")

            pbar.set_description(f"fetching isolated margin interests for {symbol}")
            self.update_margin_interests(isolated_symbol=symbol, show_pbar=False)
            pbar.update()

        pbar.close()

    def update_margin_interests(self, isolated_symbol: Optional[str] = None, show_pbar: bool = True):
        """
        Update the interests for all cross margin assets or for a isolated margin symbol if provided.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-repay-record-user_data

        :param isolated_symbol: only for isolated margin, provide the trading symbol. Otherwise cross margin data will
            be updated
        :type isolated_symbol: Optional[str]
        :param show_pbar: if the progress bar is displayed
        :type show_pbar: bool
        :return:
        :rtype:
        """
        margin_type = 'cross' if isolated_symbol is None else 'isolated'
        latest_time = self.db.get_last_margin_interest_time(isolated_symbol=isolated_symbol)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        pbar = tqdm(disable=not show_pbar)
        desc = f"fetching {margin_type} margin interests"
        if isolated_symbol is not None:
            desc = desc + f" for {isolated_symbol}"
        pbar.set_description(desc)
        while True:
            params = {
                'current': current,
                'startTime': latest_time + 1000,
                'size': 100,
                'archived': archived
            }
            if isolated_symbol is not None:
                params['isolatedSymbol'] = isolated_symbol

            # no built-in method yet in python-binance for margin/interestHistory
            client_params = {
                'method': 'get',
                'path': 'margin/interestHistory',
                'signed': True,
                'data': params
            }
            interests = self._call_binance_client('_request_margin_api', client_params)

            for interest in interests['rows']:
                self.db.add_margin_interest(interest_time=interest['interestAccuredTime'],
                                            asset=interest['asset'],
                                            interest=interest['interest'],
                                            interest_type=interest['type'],
                                            isolated_symbol=interest.get('isolatedSymbol'),
                                            auto_commit=False)

            pbar.update()
            if len(interests['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived interests
                current = 1
                archived = False
                latest_time = self.db.get_last_margin_interest_time(isolated_symbol=isolated_symbol)
            else:
                break
        pbar.close()

    def update_cross_margin_repays(self):
        """
        update the repays for all cross margin assets

        :return: None
        :rtype: None
        """
        client_params = {
            'method': 'get',
            'path': 'margin/allPairs',
            'data': {}
        }
        symbols_info = self._call_binance_client('_request_margin_api', client_params)  # not built-in yet
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

    def update_isolated_margin_repays(self, symbols_info: Optional[List[Dict]] = None):
        """
        Update the repays for isolated margin assets

        :param symbols_info: details on the symbols to fetch repays on. Each dictionary needs the fields 'asset' and
            'ref_asset'. If not provided, will update all isolated symbols.
        :type symbols_info: Optional[List[Dict]]
        :return: None
        :rtype: None
        """
        asset_key = 'asset'
        ref_asset_key = 'ref_asset'
        if symbols_info is None:
            symbols_info = self.get_margin_symbol_info(isolated=True)
            asset_key = 'base'
            ref_asset_key = 'quote'

        pbar = tqdm(total=2 * len(symbols_info))
        for symbol_info in symbols_info:
            asset = symbol_info[asset_key]
            ref_asset = symbol_info[ref_asset_key]
            symbol = symbol_info.get('symbol', f"{asset}{ref_asset}")

            pbar.set_description(f"fetching {asset} isolated margin repays for {symbol}")
            self.update_margin_asset_repay(asset=asset, isolated_symbol=symbol)
            pbar.update()

            pbar.set_description(f"fetching {ref_asset} isolated margin repays for {symbol}")
            self.update_margin_asset_repay(asset=ref_asset, isolated_symbol=symbol)
            pbar.update()
        pbar.close()

    def update_margin_asset_repay(self, asset: str, isolated_symbol: Optional[str] = None):
        """
        update the repays database for a specified asset.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-repay-record-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_repay_details

        :param asset: asset for the repays
        :type asset: str
        :param isolated_symbol: only for isolated margin, provide the trading symbol. Otherwise cross margin data will
            be updated
        :type isolated_symbol: Optional[str]
        :return: None
        :rtype: None
        """
        latest_time = self.db.get_last_repay_time(asset=asset, isolated_symbol=isolated_symbol)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        while True:
            client_params = {
                'asset': asset,
                'current': current,
                'startTime': latest_time + 1000,
                'archived': archived,
                'size': 100
            }
            if isolated_symbol is not None:
                client_params['isolatedSymbol'] = isolated_symbol
            repays = self._call_binance_client('get_margin_repay_details', client_params)

            for repay in repays['rows']:
                if repay['status'] == 'CONFIRMED':
                    self.db.add_repay(tx_id=repay['txId'],
                                      repay_time=repay['timestamp'],
                                      asset=repay['asset'],
                                      principal=repay['principal'],
                                      interest=repay['interest'],
                                      isolated_symbol=repay.get('isolatedSymbol', None),
                                      auto_commit=False)

            if len(repays['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived repays
                current = 1
                archived = False
                latest_time = self.db.get_last_repay_time(asset=asset)
            else:
                break

    def update_cross_margin_loans(self):
        """
        update the loans for all cross margin assets

        :return: None
        :rtype: None
        """
        client_params = {
            'method': 'get',
            'path': 'margin/allPairs',
            'data': {}
        }
        symbols_info = self._call_binance_client('_request_margin_api', client_params)  # not built-in yet
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

    def update_isolated_margin_loans(self, symbols_info: Optional[List[Dict]] = None):
        """
        Update the loans for isolated margin assets

        :param symbols_info: details on the symbols to fetch loans on. Each dictionary needs the fields 'asset' and
            'ref_asset'. If not provided, will update all isolated symbols.
        :type symbols_info: Optional[List[Dict]]
        :return: None
        :rtype: None
        """
        asset_key = 'asset'
        ref_asset_key = 'ref_asset'
        if symbols_info is None:
            symbols_info = self.get_margin_symbol_info(isolated=True)
            asset_key = 'base'
            ref_asset_key = 'quote'

        pbar = tqdm(total=2 * len(symbols_info))
        for symbol_info in symbols_info:
            asset = symbol_info[asset_key]
            ref_asset = symbol_info[ref_asset_key]
            symbol = symbol_info.get('symbol', f"{asset}{ref_asset}")

            pbar.set_description(f"fetching {asset} isolated margin loans for {symbol}")
            self.update_margin_asset_loans(asset=asset, isolated_symbol=symbol)
            pbar.update()

            pbar.set_description(f"fetching {ref_asset} isolated margin loans for {symbol}")
            self.update_margin_asset_loans(asset=ref_asset, isolated_symbol=symbol)
            pbar.update()

        pbar.close()

    def update_margin_asset_loans(self, asset: str, isolated_symbol: Optional[str] = None):
        """
        update the loans database for a specified asset.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-loan-record-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_loan_details

        :param asset: asset for the loans
        :type asset: str
        :param isolated_symbol: only for isolated margin, provide the trading symbol. Otherwise cross margin data will
            be updated
        :type isolated_symbol: Optional[str]
        :return: None
        :rtype: None
        """
        latest_time = self.db.get_last_loan_time(asset=asset, isolated_symbol=isolated_symbol)
        archived = 1000 * time.time() - latest_time > 1000 * 3600 * 24 * 30 * 3
        current = 1
        while True:
            client_params = {
                'asset': asset,
                'current': current,
                'startTime': latest_time + 1000,
                'archived': archived,
                'size': 100
            }
            if isolated_symbol is not None:
                client_params['isolatedSymbol'] = isolated_symbol
            loans = self._call_binance_client('get_margin_loan_details', client_params)

            for loan in loans['rows']:
                if loan['status'] == 'CONFIRMED':
                    self.db.add_loan(tx_id=loan['txId'],
                                     loan_time=loan['timestamp'],
                                     asset=loan['asset'],
                                     principal=loan['principal'],
                                     isolated_symbol=loan.get('isolatedSymbol'),
                                     auto_commit=False)

            if len(loans['rows']):
                current += 1  # next page
                self.db.commit()
            elif archived:  # switching to non archived loans
                current = 1
                archived = False
                latest_time = self.db.get_last_loan_time(asset=asset, isolated_symbol=isolated_symbol)
            else:
                break

    def update_margin_symbol_trades(self, asset: str, ref_asset: str, is_isolated: bool = False, limit: int = 1000):
        """
        This update the margin trades in the database for a single trading pair.
        It will check the last trade id and will requests the all trades after this trade_id.

        sources:
        https://binance-docs.github.io/apidocs/spot/en/#query-margin-account-39-s-trade-list-user_data
        https://python-binance.readthedocs.io/en/latest/binance.html#binance.client.Client.get_margin_trades

        :param asset: name of the asset in the trading pair (ex 'BTC' for 'BTCUSDT')
        :type asset: string
        :param ref_asset: name of the reference asset in the trading pair (ex 'USDT' for 'BTCUSDT')
        :type ref_asset: string
        :param is_isolated: if margin type is isolated, default False
        :type is_isolated: bool
        :param limit: max size of each trade requests
        :type limit: int
        :return: None
        :rtype: None
        """
        trade_type = 'isolated_margin' if is_isolated else 'cross_margin'
        limit = min(1000, limit)
        symbol = asset + ref_asset
        last_trade_id = self.db.get_max_trade_id(asset, ref_asset, trade_type)
        while True:
            client_params = {
                'symbol': symbol,
                'fromId': last_trade_id + 1,
                'isIsolated': is_isolated,
                'limit': limit
            }
            new_trades = self._call_binance_client('get_margin_trades', client_params)

            for trade in new_trades:
                self.db.add_trade(trade_type=trade_type,
                                  trade_id=int(trade['id']),
                                  trade_time=int(trade['time']),
                                  asset=asset,
                                  ref_asset=ref_asset,
                                  qty=float(trade['qty']),
                                  price=float(trade['price']),
                                  fee=float(trade['commission']),
                                  fee_asset=trade['commissionAsset'],
                                  is_buyer=trade['isBuyer'],
                                  symbol=symbol,
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
        client_params = {
            'method': 'get',
            'path': 'margin/allPairs',
            'data': {}
        }
        symbols_info = self._call_binance_client('_request_margin_api', client_params)  # not built-in yet

        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            pbar.set_description(f"fetching {symbol_info['symbol']} cross margin trades")
            self.update_margin_symbol_trades(asset=symbol_info['base'],
                                             ref_asset=symbol_info['quote'],
                                             limit=limit)
            pbar.update()
        pbar.close()

    def update_isolated_margin_trades(self, symbols_info: Optional[List[Dict]] = None):
        """
        This update the isolated margin trades in the database for every trading pairs

        :param symbols_info: details on the symbols to fetch trades on. Each dictionary needs the fields 'asset' and
            'ref_asset'. If not provided, will update all isolated symbols.
        :type symbols_info: Optional[List[Dict]]
        :return: None
        :rtype: None
        """
        asset_key = 'asset'
        ref_asset_key = 'ref_asset'
        if symbols_info is None:
            symbols_info = self.get_margin_symbol_info(isolated=True)
            asset_key = 'base'
            ref_asset_key = 'quote'

        pbar = tqdm(total=len(symbols_info))
        for symbol_info in symbols_info:
            asset = symbol_info[asset_key]
            ref_asset = symbol_info[ref_asset_key]
            symbol = symbol_info.get('symbol', f"{asset}{ref_asset}")
            pbar.set_description(f"fetching {symbol} isolated margin trades")

            try:
                self.update_margin_symbol_trades(asset=asset,
                                                 ref_asset=ref_asset,
                                                 limit=1000,
                                                 is_isolated=True)
            except BinanceAPIException as e:
                if e.code != -11001:  # -11001 means that this isolated pair has never been used
                    raise e
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
                client_params = {
                    'lendingType': lending_type,
                    'startTime': latest_time,
                    'current': current,
                    'size': 100
                }
                lending_redemptions = self._call_binance_client('get_lending_redemption_history', client_params)

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
                client_params = {
                    'lendingType': lending_type,
                    'startTime': latest_time,
                    'current': current,
                    'size': 100
                }
                lending_purchases = self._call_binance_client('get_lending_purchase_history', client_params)

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
                client_params = {
                    'lendingType': lending_type,
                    'startTime': latest_time,
                    'current': current,
                    'size': 100
                }
                lending_interests = self._call_binance_client('get_lending_interest_history', client_params)

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

        result = self._call_binance_client('get_dust_log')
        dusts = result['results']
        pbar = tqdm(total=dusts['total'])
        pbar.set_description("fetching spot dusts")
        for d in dusts['rows']:
            for sub_dust in d['logs']:
                date_time = dateparser.parse(sub_dust['operateTime'] + 'Z')
                self.db.add_spot_dust(tran_id=sub_dust['tranId'],
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
            # the stable working version of client.get_asset_dividend_history is not released yet,
            # for now it has a post error, so this protected member is used in the meantime
            params = {
                'startTime': start_time,
                'endTime': start_time + delta_jump,
                'limit': limit
            }
            client_params = {
                'method': 'get',
                'path': 'asset/assetDividend',
                'signed': True,
                'data': params
            }
            result = self._call_binance_client('_request_margin_api', client_params)

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
                start_time += delta_jump + 1  # endTime is included in the previous return, so we have to add 1
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
            client_params = {
                'startTime': start_time,
                'endTime': start_time + delta_jump,
                'status': 6
            }
            result = self._call_binance_client('get_withdraw_history', client_params)

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
            start_time += delta_jump + 1  # endTime is included in the previous return, so we have to add 1
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
            client_params = {
                'startTime': start_time,
                'endTime': start_time + delta_jump,
                'status': 1
            }
            result = self._call_binance_client('get_deposit_history', client_params)

            deposits = result['depositList']
            for deposit in deposits:
                self.db.add_deposit(tx_id=deposit['txId'],
                                    asset=deposit['asset'],
                                    insert_time=int(deposit['insertTime']),
                                    amount=float(deposit['amount']),
                                    auto_commit=False)
            pbar.update()
            start_time += delta_jump + 1  # endTime is included in the previous return, so we have to add 1
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
            client_params = {
                'symbol': symbol,
                'fromId': last_trade_id + 1,
                'limit': limit
            }
            new_trades = self._call_binance_client('get_my_trades', client_params)

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

    def _call_binance_client(self, method_name: str, params: Optional[Dict] = None,
                             retry_count: int = 0) -> Union[Dict, List]:
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
        :rtype: Union[Dict, List]
        """
        if params is None:
            params = dict()
        if retry_count >= BinanceManager.API_MAX_RETRY:
            raise RuntimeError(f"The API rate limits has been breached {retry_count} times")

        try:
            return getattr(self.client, method_name)(**params)
        except BinanceAPIException as err:
            if err.code == -1003:  # API rate Limits
                # wait_time = float(err.response.headers['Retry-After']) it seems to be always 0, so unusable
                wait_time = 1 + 60 - datetime.datetime.now().timestamp() % 60  # number of seconds until next minute
                if err.response.status_code == 418:  # ban
                    self.logger.error(f"API calls resulted in a ban, retry in {wait_time} seconds")
                    raise err
                self.logger.info(f"API calls resulted in a breach of rate limits,"
                                 f" will retry after {wait_time:.2f} seconds")
                time.sleep(wait_time)
                return self._call_binance_client(method_name, params, retry_count + 1)
            raise err
