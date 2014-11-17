simplecoin_rpc_client
=====================

The RPC (mostly payout operations) client for simplecoin.

Installation
============

Better for dev work/live changes:

```
pip install -e .
```

or

```
python setup.py install
```

Basic Usage
===========
The basic work flow for payouts should look like this

```
SCM: Creates payouts (shows 'payout pending' on SCM user stats) ->
SCRPC: Pull the payouts + record in local DB ->
SCRPC: Actually send the network TX ->
SCRPC: Update SCM with payout TX id (shows 'Funds sent' on SCM user stats) ->
SCRPC: Confirm the transactions on the network (shows 'Complete' on SCM user stats) ->
```


The basic work flow for trade requests looks like this

```
SCM: Create sell request(s) ->
SCRPC: get trade requests + sell appropriate amount on exchanges ->
SCRPC: Close sell request ->
SCM: Create buy request(s) ->
SCRPC: get trade requests + buy appropriate amount on exchanges ->
SCRPC: Close buy request ->
SCM: Create payouts ->
```

Automatic payout cron
---------------------

```
python simplecoin_rpc_client/scheduler.py
```

Manual payout
-------------

Pull the payouts + record in local DB (`pull_payouts()`):
```
python simplecoin_rpc_client/manage.py  -f pull_payouts -cl /config.yml -l DEBUG -c [CURRENCY] -a simulate=True
```

Actually send the network TX (`send_payout()`):
```
python simplecoin_rpc_client/manage.py  -f send_payout -cl /config.yml -l DEBUG -c [CURRENCY] -a simulate=True
```

Update SCM with payout TX id (`associate()/associate_all()`):
```
python simplecoin_rpc_client/manage.py  -f associate_all -cl /config.yml -l DEBUG -c [CURRENCY] -a simulate=True
```

Confirm the transactions on the network (`confirm_trans()`):
```
python simplecoin_rpc_client/manage.py  -f confirm_trans -cl /config.yml -l DEBUG -c [CURRENCY] -a simulate=True
```


Manually manage trade requests
------------------------------

List trade requests
```
python simplecoin_rpc_client/manage.py  -f get_open_trade_requests -cl /config.yml -l DEBUG -c [CURRENCY]
```

Close a sell request
```
python simplecoin_rpc_client/manage.py  -f close_trade_request -cl /config.yml -l DEBUG -a [TR_ID] [BTC_FROM_SALE] [FEES(BTC)] simulate=True -c [CURRENCY]
```

Close a buy request

```
python simplecoin_rpc_client/manage.py  -f close_trade_request -cl /config.yml -l DEBUG -a [TR_ID] [CUR_BOUGHT] [FEES(CUR)] simulate=True -c [CURRENCY]
```
