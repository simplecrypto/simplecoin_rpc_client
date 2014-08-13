import os
import logging
import sys
import argparse
import time
import requests
import decimal

from urlparse import urljoin
from cryptokit.base58 import get_bcaddress_version
from itsdangerous import TimedSerializer, BadData

from bitcoinrpc.authproxy import JSONRPCException, CoinRPCException
from .coinserv_cmds import payout_many


class RPCException(Exception):
    pass


class RPCClient(object):
    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        self.config = dict(coinservs=None,
                           valid_address_versions=[],
                           max_age=10,
                           rpc_signature=None,
                           logger_name="rpc",
                           log_path=os.path.abspath(os.path.dirname(__file__) + '/../') + '/rpc.log')
        self.config.update(kwargs)

        # check that we have at least one configured coin server
        if not self.config['coinservs']:
            self.logger.error("Shit won't work without a coinserver to connect to")
            exit(1)

        if not self.config['valid_address_versions']:
            self.logger.error("We need address versions to validate payout amounts")
            exit(1)

        if not self.config['rpc_signature']:
            self.logger.error("Can't send/recieve rpc commands without a rpc_signature config value")
            exit(1)

    def __init__(self, config):
        self._set_config(config)

        self.logger = logging.getLogger(self.config['logger_name'])
        self.logger.setLevel(getattr(logging, self.config['log_level']))
        log_format = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

        # stdout handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(log_format)
        handler.setLevel(getattr(logging, self.config['log_level']))
        self.logger.addHandler(handler)

        # don't attach a file handler if path evals false
        if self.config['log_path']:
            handler = logging.FileHandler(self.config['log_path'])
            handler.setFormatter(log_format)
            handler.setLevel(getattr(logging, self.config['log_level']))
            self.logger.addHandler(handler)

        self.serializer = TimedSerializer(self.config['rpc_signature'])

    def post(self, url, *args, **kwargs):
        if 'data' not in kwargs:
            kwargs['data'] = ''
        kwargs['data'] = self.serializer.dumps(kwargs['data'])
        return self.remote(url, 'post', *args, **kwargs)

    def get(self, url, *args, **kwargs):
        return self.remote(url, 'get', *args, **kwargs)

    def remote(self, url, method, max_age=None, signed=True, **kwargs):
        url = urljoin(self.config['rpc_url'], url)
        self.logger.debug("Making request to {}".format(url))
        ret = getattr(requests, method)(url, timeout=270, **kwargs)
        if ret.status_code != 200:
            raise RPCException("Non 200 from remote: {}".format(ret.text))

        try:
            self.logger.debug("Got {} from remote".format(ret.text.encode('utf8')))
            if signed:
                return self.serializer.loads(ret.text, max_age or self.max_age)
            else:
                return ret.json()
        except BadData:
            self.logger.error("Invalid data returned from remote!", exc_info=True)
            raise RPCException("Invalid signature")

    def poke_rpc(self, conn):
        try:
            conn.getinfo()
        except JSONRPCException:
            raise RPCException("Coinserver not awake")

    def confirm_trans(self, simulate=False):
        """ Grabs the unconfirmed transactions objects from the remote server
        and checks if they're confirmed. Also grabs and pushes the fees for the
        transaction if remote server supports it. """
        self.poke_rpc(self.coinserv)

        res = self.get('api/transaction?__filter_by={{"confirmed":false,"merged_type":{}}}'
                       .format(self.config['currency_code']), signed=False)

        if not res['success']:
            self.logger.error("Failure grabbing unconfirmed transactions: {}".format(res))
            return

        tids = []
        fees = {}
        for obj in res['objects']:
            self.logger.debug("Connecting to coinserv to lookup confirms for {}"
                              .format(obj['txid']))
            try:
                trans_data = self.coinserv.gettransaction(obj['txid'])
            except CoinRPCException:
                self.logger.error("Unable to fetch txid {} from rpc server!"
                                  .format(obj['txid']))
            except Exception:
                self.logger.error("Unable to fetch txid {} from rpc server!"
                                  .format(obj['txid']), exc_info=True)
            else:
                if trans_data['confirmations'] > self.config['trans_confirmations']:
                    tids.append(obj['txid'])
                    self.logger.info("Confirmed txid {} with {} confirms"
                                     .format(obj['txid'], trans_data['confirmations']))

                # grab and populate fee value if:
                # 1. Key is present in json from remote api (reverse compat)
                # 2. Key is not populated
                # 3. We got back a valid fee value from the rpc server
                if 'fee' in trans_data and not obj.get("fee") and 'fee' in obj:
                    assert isinstance(trans_data['fee'], decimal.Decimal)
                    fees[obj['txid']] = int(trans_data['fee'] * 100000000)
                    self.logger.info("Pushing fee value {} for txid {}"
                                     .format(trans_data['fee'], obj['txid']))

        if tids or fees:
            data = {'tids': tids, 'fees': fees}
            if self.post('confirm_transactions', data=data):
                self.logger.info("Sucessfully confirmed transactions")  # XXX: Add number print outs
                return True

            self.logger.error("Failed to push confirmation information")
            return False
        else:
            self.logger.info("No valid transactions in need of fee value or confirmation")

    def reset_trans(self, pids, simulate=False):
        """ Resets a list of pids and bids """
        data = {'pids': pids, 'reset': True}
        self.logger.info("Resetting {:,} payout ids".format(len(pids)))
        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        self.post('update_payouts', data=data)

    def associate_trans(self, pids, transaction_id, simulate=False):
        data = {'coin_txid': transaction_id, 'pids': pids, 'merged': self.config['currency_code']}
        self.logger.info("Associating {:,} payout ids and with txid {}"
                         .format(len(pids), transaction_id))

        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        if self.post('update_payouts', data=data):
            self.logger.info("Sucessfully associated!")
            return True

        self.logger.error("Failed to associate!")
        return False

    def pull_payouts(self, simulate=False, datadir=None):
        """ Gets all the unpaid payouts from the server """
        lock = not simulate
        payouts, bonus_payouts, lock_res = self.post(
            'get_payouts',
            data={'lock': lock, 'merged': self.config['currency_code']}
        )
        if lock:
            assert lock_res

        pids = [t[2] for t in payouts]

        if not len(pids):
            self.logger.info("No payouts to process.. End proc_trans")
            return True

        if not simulate:
            # XXX: Insert into SQLAlchemy the payout information. Filter invalid addresses
            #if get_bcaddress_version(payout.user) in self.config['valid_address_versions']:
            #    pass
            pass

        self.logger.info("Recieved {:,} payouts from the server".format(len(pids)))

    def payout(self, simulate=False):
        """ Collects all the unpaid payout ids and pays them out """
        self.poke_rpc(self.coinserv)

        # XXX: Grab all the unpaid from db
        payouts = []

        # builds two dictionaries, one that tracks the total payouts to a user,
        # and another that tracks all the payout ids (pids)
        user_payout_amounts = {}
        pids = []
        for payout in payouts:
            user_payout_amounts.setdefault(payout.user, 0)
            user_payout_amounts[payout.user] += payout.amount
            payout.locked = True
            pids.append(payout.pid)

        # XXX: Perform a complete filesync of the lock state commit here!
        total_out = sum(user_payout_amounts.values())
        self.logger.info("Trying to payout a total of {}"
                         .format(total_out))

        if not simulate:
            # XXX: Check current wallet balance and stop if needed. Record balance
            pass

        self.logger.info("To be paid")
        self.logger.info(user_payout_amounts)

        self.logger.info("List of payout ids to be committed")
        self.logger.info(pids)

        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            exit(0)

        try:
            # now actually pay them
            coin_txid = payout_many(user_payout_amounts)
            #coin_txid = "1111111111111111111111111111111111111111111111111111111111111111"
        except CoinRPCException as e:
            if isinstance(e.error, dict) and e.error.get('message') == 'Insufficient funds':
                self.logger.error("Insufficient funds, reseting...")
                self.reset_trans(pids)
            else:
                self.logger.error("Unkown RPC error, you'll need to manually reset the payouts", exc_info=True)
            # XXX: Raise exception
        else:
            for payout in payouts:
                payout.locked = False
                payout.txid = coin_txid

            return coin_txid

    def associate_all(self, simulate=False):
        # XXX: Grab all the unassciated payouts from SQLALCHEMY
        payouts = []
        txids = {}
        for payout in payouts:
            txids.setdefault(payout.txid, [])
            txids[payout.txid].append(payout)

        # XXX: Consider scheduling these as tasks?
        for txid, payouts in txids.iteritems():
            self.associate(txid, payouts, simulate=simulate)

    def associate(self, txid, payouts, simulate=False):
        pids = [p.pid for p in payouts]
        self.logger.info("Got {} as txid for payout, now pushing result to server!"
                         .format(txid))

        retries = 0
        while retries < 5:
            try:
                if self.associate_trans(pids, txid, merged=self.config['currency_code']):
                    self.logger.info("Recieved success response from the server.")
                    for payout in self.payouts:
                        payout.associated = True
                        self.db.session.commit()
                    break
            except Exception:
                self.logger.error("Server returned failure response, retrying "
                                  "{} more times.".format(4 - retries), exc_info=True)
            retries += 1
            time.sleep(15)

    def payout_many(self, recip):
        self.coinserv = self.coinserv
        fee = self.config['payout_fee']
        passphrase = self.config['coinserv']['wallet_pass']
        account = self.config['coinserv']['account']

        if passphrase:
            wallet = self.coinserv.walletpassphrase(passphrase, 10)
            self.logger.info("Unlocking wallet: %s" % wallet)
        self.logger.info("Setting tx fee: %s" % self.coinserv.settxfee(fee))
        self.logger.info("Sending to recip: " + str(recip))
        self.logger.info("Sending from account: " + str(account))
        return self.coinserv.sendmany(account, recip)


def entry():
    parser = argparse.ArgumentParser(prog='simplecoin RPC')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    parser.add_argument('-s', '--simulate', action='store_true', default=False)
    subparsers = parser.add_subparsers(title='main subcommands', dest='action')

    subparsers.add_parser('confirm_trans',
                          help='fetches unconfirmed transactions and tries to confirm them')
    proc = subparsers.add_parser('proc_trans',
                                 help='processes transactions locally by '
                                      'fetching from a remote server')
    proc.add_argument('-m', '--merged', default=None)
    proc.add_argument('-d', '--datadir', required=True,
                      help='a folder that data will be stored in for resetting failed transactions')
    reset = subparsers.add_parser('reset_trans',
                                  help='resets the lock state of a set of pids'
                                       ' and bids')
    reset.add_argument('pids')
    reset.add_argument('bids')
    reset_file = subparsers.add_parser('reset_trans_file',
                                       help='resets the lock state of a set of pids'
                                       ' and bids by providing json file')
    reset_file.add_argument('fo', type=argparse.FileType('r'))

    confirm = subparsers.add_parser('associate_trans',
                                    help='associates pids/bids with transactions')
    confirm.add_argument('pids')
    confirm.add_argument('bids')
    confirm.add_argument('transaction_id')
    confirm.add_argument('merged')
    confirm_file = subparsers.add_parser('associate_trans_file',
                                         help='associates bids/pids with a txid by providing json file')
    confirm_file.add_argument('fo', type=argparse.FileType('r'))
    args = parser.parse_args()

    ch.setLevel(getattr(logging, args.log_level))
    logger.setLevel(getattr(logging, args.log_level))

    global_args = ['log_level', 'action']
    # subcommand functions shouldn't recieve arguments directed at the
    # global object/ configs
    kwargs = {k: v for k, v in vars(args).iteritems() if k not in global_args}

    interface = RPCClient()
    try:
        getattr(interface, args.action)(**kwargs)
    except requests.exceptions.ConnectionError:
        logger.error("Couldn't connect to remote server", exc_info=True)
    except JSONRPCException as e:
        logger.error("Recieved exception from rpc server: {}"
                        .format(getattr(e, 'error')))
