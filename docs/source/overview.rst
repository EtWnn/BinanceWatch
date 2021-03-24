Getting Started
===============

Installation
------------

``BinanceWatch`` is available on `PYPI <https://pypi.org/project/BinanceWatch/>`_, install with ``pip``:

.. code:: bash

    pip install BinanceWatch

Register on Binance
-------------------

If you are interested in this library, I assume that you have already a Binance account. If not you can 
`register an account with Binance <https://www.binance.com/en/register?ref=40934070&utm_campaign=web_share_copy>`_.

Generate an API Key
-------------------

To use signed account methods you are required to `create an API Key  <https://www.binance.com/en/my/settings/api-management>`_.
In this library, only read permissions are needed so don't forget to disabled the others restrictions (trading, withdrawal ...)

Initialise the manager
----------------------

Pass your API Key and Secret to the manager

.. code:: python

    from BinanceWatch.BinanceManager import BinanceManager
    bm = BinanceManager(api_key, api_secret)

API calls
---------

All the API calls to Binance are handled by the library `python-binance <https://python-binance.readthedocs.io/en/latest/>`_.
Don't hesitate to check their very useful `githib repo <https://github.com/sammchardy/python-binance>`_ and drop a star!

Updates
-------

The manager is mainly used to update the transactions saved locally. By calling an update method from the
manager, it will check if any transaction has been made between the last one saved locally and the current time.
The details of the update methods are in the section :doc:`binance_manager`.

Retrievals
----------

Each manager has a database, which is where the results of the Binance API calls are stored. By calling
the get methods of the database, you will retrieve the history of your Binance account. See the :doc:`binance_database`
section for more details.

Examples
--------

You can updates the elements by type, for example here with the crypto spot deposits:

.. code-block:: python

    bm.update_spot_deposits()  # will fetch the latest deposits not saved locally

    bm.db.get_spot_deposits()  # return the deposits saved locally

.. code-block:: bash

    [
        ('azdf5e6a1d5z',    # transaction id
        1589479004000,      # deposit time
        'LTC',              # asset
        14.25),             # amount
    ...
    ]

You can also use larger update methods, that will update several types of elements.
Below the method will update every elements of a cross margin account:

.. code-block:: python

    bm.update_cross_margin()  # will fetch the latest transfers, trades, loans ...

    bm.db.get_trades(trade_type='cross_margin')

.. code-block:: bash

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
    ...
    ]