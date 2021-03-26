==============================
Welcome to BinanceWatch v0.1.2
==============================

Note
----


This library is under development by EtWnn, feel free to drop your suggestions or remarks in
the discussion tab of the git repo. You are also welcome to contribute by submitting PRs.

This is an unofficial tracker for binance accounts. I am in no way affiliated with Binance, use at
your own risk.

**Source Code:**
    https://github.com/EtWnn/BinanceWatch
**Documentation:**
    https://binancewatch.readthedocs.io/en/latest/


Features
--------


If you used quite intensively Binance, it can take some time to retrieve everything that happened
on your account. This library is made to save locally the events of your account so that you don't
need to fetch your history from the beginning every time.


It currently supports:

- Spot Trades
- Spot Crypto Deposits
- Spot Crypto Withdraws
- Spot Dividends
- Spot Dusts
- Universal Transfers



- Lending Purchases
- Lending Interests
- Lending Redemptions



- Cross Margin Trades
- Cross Margin Repayment
- Cross Margin Loans
- Cross Margin Interests

Quick Tour
----------


`Generate an API Key <https://www.binance.com/en/my/settings/api-management>`_ in your binance account. Only read
permissions are needed.

``BinanceWatch`` is available on `PYPI <https://pypi.org/project/BinanceWatch/>`_, install with ``pip``:

.. code:: bash

    pip install BinanceWatch

.. code:: python

    from BinanceWatch.BinanceManager import BinanceManager

    api_key = "<API_KEY>"
    api_secret = "<API_SECRET>"

    bm = BinanceManager(api_key, api_secret)

    # fetch the latest spot trades from Binance
    bm.update_all_spot_trades()

.. code:: bash

    Out -> fetching BIFIBUSD: 100%|██████████████████████| 1349/1349 [06:24<00:00,  3.51it/s]

.. code:: python

    from datetime import datetime
    from BinanceWatch.utils.time_utils import datetime_to_millistamp


    start_time = datetime_to_millistamp(datetime(2018,1,1))

    # get the locally saved spot trades made after 2018/01/01
    spot_trades = bm.db.get_trades('spot', start_time=start_time)


You can also call update functions at an account-type level, and it will call every update
methods related to this account-type:

.. code:: python

    bm.update_spot()  # (trades, transfers, deposits ...)

    bm.update_cross_margin()  # (trades, loans, repays, interests...)

    bm.update_lending()  # (purchases, interests, redemptions..)


Donation
--------


If this library has helped you in any way, feel free to donate:

- **BTC**: 14ou4fMYoMVYbWEKnhADPJUNVytWQWx9HG
- **ETH**: 0xfb0ebcf8224ce561bfb06a56c3b9a43e1a4d1be2
- **LTC**: LfHgc969RFUjnmyLn41SRDvmT146jUg9tE
- **EGLD**: erd1qk98xm2hgztvmq6s4jwtk06g6laattewp6vh20z393drzy5zzfrq0gaefh


Known Issues:
-------------


Some endpoints are not yet provided by Binance, so they can't be implemented in this library:

- Fiat withdraws and deposits
- Locked stacking history
- Direct purchases with debit card
