from sc_trader.utils.exceptions import CoinRPCException
from urllib3.exceptions import ConnectionError
import yaml
import time
import os
import logging
import sys
import argparse
import datetime
import requests
from decimal import Decimal as Dec

from tabulate import tabulate
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy as sa

from urlparse import urljoin
from cryptokit.base58 import get_bcaddress_version
from itsdangerous import TimedSerializer, BadData


base = declarative_base()


class Payout(base):
    """ Our single table in the sqlite database. Handles tracking the status of
    payouts and keeps track of tasks that needs to be retried, etc. """
    __tablename__ = "payouts"
    id = sa.Column(sa.Integer, primary_key=True)
    pid = sa.Column(sa.String, unique=True, nullable=False)
    user = sa.Column(sa.String, nullable=False)
    # SQLlite does not have support for Decimal - use STR instead
    amount = sa.Column(sa.String, nullable=False)
    currency_code = sa.Column(sa.String, nullable=False)
    txid = sa.Column(sa.String)
    associated = sa.Column(sa.Boolean, default=False, nullable=False)
    locked = sa.Column(sa.Boolean, default=False, nullable=False)

    # Times
    lock_time = sa.Column(sa.DateTime)
    paid_time = sa.Column(sa.DateTime)
    assoc_time = sa.Column(sa.DateTime)
    pull_time = sa.Column(sa.DateTime)

    @property
    def trans_id(self):
        if self.txid is None:
            return "NULL"
        return self.txid

    @property
    def amount_float(self):
        return float(self.amount)

    def tabulize(self, columns):
        return [getattr(self, a) for a in columns]


class SCRPCException(Exception):
    pass


class SCRPCClient(object):
    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        base = os.path.abspath(os.path.dirname(__file__) + '/../')
        self.config = dict(valid_address_versions=[],
                           max_age=10,
                           logger_name="sc_rpc_client",
                           log_level="INFO",
                           database_path=base + '/rpc_',
                           log_path=base + '/sc_rpc.log',
                           min_tx_confirms=12,
                           wallet_account=None,
                           minimum_tx_output=0.00000001)
        self.config.update(kwargs)

        # Kinda sloppy, but it works
        self.config['database_path'] += self.config['currency_code'] + '.sqlite'

        required_conf = ['valid_address_versions', 'currency_code',
                         'rpc_signature', 'rpc_url']
        error = False
        for req in required_conf:
            if req not in self.config:
                print("{} is a required configuration variable".format(req))
                error = True

        if error:
            raise SCRPCException('Errors occurred while configuring RPCClient obj')

    def __init__(self, config, CoinRPC, flask_app=None):

        if not config:
            raise SCRPCException('Invalid configuration file')
        self._set_config(**config)

        # Setup CoinRPC
        self.coin_rpc = CoinRPC

        # Setup the sqlite database mapper
        self.engine = sa.create_engine('sqlite:///{}'.format(self.config['database_path']),
                                  echo=self.config['log_level'] == "DEBUG")

        # Pulled from SQLA docs to implement strict exclusive access to the
        # payout state database.
        # See http://docs.sqlalchemy.org/en/rel_0_9/dialects/sqlite.html#pysqlite-serializable
        @sa.event.listens_for(self.engine, "connect")
        def do_connect(dbapi_connection, connection_record):
            # disable pysqlite's emitting of the BEGIN statement entirely.
            # also stops it from emitting COMMIT before any DDL.
            dbapi_connection.isolation_level = None

        @sa.event.listens_for(self.engine, "begin")
        def do_begin(conn):
            # emit our own BEGIN
            conn.execute("BEGIN EXCLUSIVE")

        self.db = sessionmaker(bind=self.engine)
        self.db.session = self.db()
        # Hack if flask is in the env
        self.db.session._model_changes = {}
        # Create the table if it doesn't exist
        Payout.__table__.create(self.engine, checkfirst=True)

        # Setup logger for the class
        if flask_app:
            self.logger = flask_app.logger
        # logging.Formatter.converter = time.gmtime
        # self.logger = logging.getLogger(self.config['logger_name'])
        # self.logger.setLevel(getattr(logging, self.config['log_level']))
        # log_format = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        #
        # # stdout handler
        # handler = logging.StreamHandler(sys.stdout)
        # handler.setFormatter(log_format)
        # handler.setLevel(getattr(logging, self.config['log_level']))
        # self.logger.addHandler(handler)
        #
        # # don't attach a file handler if path evals false
        # if self.config['log_path']:
        #     handler = logging.FileHandler(self.config['log_path'])
        #     handler.setFormatter(log_format)
        #     handler.setLevel(getattr(logging, self.config['log_level']))
        #     self.logger.addHandler(handler)

        self.serializer = TimedSerializer(self.config['rpc_signature'])

    ########################################################################
    # Helper URL methods
    ########################################################################
    def post(self, url, *args, **kwargs):
        if 'data' not in kwargs:
            kwargs['data'] = ''
        kwargs['data'] = self.serializer.dumps(kwargs['data'])
        return self.remote('/rpc/' + url, 'post', *args, **kwargs)

    def get(self, url, *args, **kwargs):
        return self.remote(url, 'get', *args, **kwargs)

    def remote(self, url, method, max_age=None, signed=True, **kwargs):
        url = urljoin(self.config['rpc_url'], url)
        self.logger.debug("Making request to {}".format(url))
        ret = getattr(requests, method)(url, timeout=270, **kwargs)
        if ret.status_code != 200:
            raise SCRPCException("Non 200 from remote: {}".format(ret.text))

        try:
            self.logger.debug("Got {} from remote".format(ret.text.encode('utf8')))
            if signed:
                return self.serializer.loads(ret.text, max_age or self.config['max_age'])
            else:
                return ret.json()
        except BadData:
            self.logger.error("Invalid data returned from remote!", exc_info=True)
            raise SCRPCException("Invalid signature")

    ########################################################################
    # RPC Client methods
    ########################################################################
    def pull_payouts(self, simulate=False):
        """ Gets all the unpaid payouts from the server """
        try:
            payouts = self.post(
                'get_payouts',
                data={'currency': self.config['currency_code']}
            )['pids']
        except ConnectionError:
            self.logger.warn('Unable to connect to SC!', exc_info=True)
            return

        if not payouts:
            self.logger.info("No payouts to process..")
            return

        repeat = 0
        new = 0
        invalid = 0
        for user, amount, pid in payouts:
            # Check address is valid
            if not get_bcaddress_version(user) in self.config['valid_address_versions']:
                self.logger.warn("Ignoring payout {} due to invalid address. "
                                 "{} address did not match a valid version {}"
                                 .format((user, amount, pid), self.config['currenct_code'],
                                         self.config['valid_address_versions']))
                invalid += 1
                continue
            # Check payout doesn't already exist
            if self.db.session.query(Payout).filter_by(pid=pid).first():
                self.logger.debug("Ignoring payout {} because it already exists"
                                  " locally".format((user, amount, pid)))
                repeat += 1
                continue
            # Create local payout obj
            p = Payout(pid=pid, user=user, amount=amount,
                       currency_code=self.config['currency_code'],
                       pull_time=datetime.datetime.utcnow())
            new += 1

            if not simulate:
                self.db.session.add(p)

        self.db.session.commit()

        self.logger.info("Inserted {:,} new payouts and skipped {:,} old "
                         "payouts from the server. {:,} payouts with invalid addresses."
                         .format(new, repeat, invalid))
        return True

    def send_payout(self, simulate=False):
        """ Collects all the unpaid payout ids (for the configured currency)
        and pays them out """
        self.coin_rpc.poke_rpc()

        # Grab all payouts now so that we use the same list of payouts for both
        # database transactions (locking, and unlocking)
        payouts = (self.db.session.query(Payout).
                   filter_by(txid=None,
                             locked=False,
                             currency_code=self.config['currency_code'])
                   .all())

        if not payouts:
            self.logger.info("No payouts to process, exiting")
            return True

        # track the total payouts to each user
        user_payout_amounts = {}
        pids = {}
        for payout in payouts:
            user_payout_amounts.setdefault(payout.user, 0.0)
            user_payout_amounts[payout.user] += float(payout.amount)
            pids.setdefault(payout.user, [])
            pids[payout.user].append(payout.pid)

            # We'll lock the payout before continuing in case of a failure in
            # between paying out and recording that payout action
            payout.locked = True
            payout.lock_time = datetime.datetime.utcnow()

        for user, amount in user_payout_amounts.items():
            # Convert amount from STR and coerce to a payable value.
            # Note that we're not trying to validate the amount here, all
            # validation should be handled server side.
            amount = round(float(amount), 8)

            if amount < self.config['minimum_tx_output']:
                # We're unable to pay, so undo the changes from the last loop
                self.logger.warn('Removing {} with payout amount of {} (which '
                                 'is lower than network output min of {}) from '
                                 'the {} payout dictionary'
                                 .format(user, amount,
                                         self.config['minimum_tx_output'],
                                         self.config['currency_code']))

                user_payout_amounts[user] = 0
                pids[user] = []
                for payout in payouts:
                    if payout.user == user:
                        payout.locked = False
                        payout.lock_time = None
            else:
                user_payout_amounts[user] = amount

        total_out = sum(user_payout_amounts.values())
        balance = self.coin_rpc.get_balance(self.config['wallet_account'])
        self.logger.info("Account balance for {} account \'{}\': {:,}".format(self.config['currency_code'], self.config['wallet_account'],
                                                                   balance))
        self.logger.info("Total to be paid {:,}".format(total_out))

        if balance < total_out:
            self.logger.error("Payout wallet is out of funds!")
            self.db.session.rollback()
            # XXX: Add an email call here
            return False

        if not simulate:
            self.db.session.commit()
        else:
            self.db.session.rollback()

        def format_pids(pids):
            lst = ", ".join(pids[:9])
            if len(pids) > 9:
                return lst + "... ({} more)".format(len(pids) - 8)
            return lst
        summary = [(user, amount, format_pids(upids)) for
                   (user, amount), upids in zip(user_payout_amounts.iteritems(), pids.itervalues())]

        self.logger.info(
            "User payment summary\n" + tabulate(summary, headers=["User", "Total", "Pids"], tablefmt="grid"))

        try:
            if simulate:
                coin_txid = "1111111111111111111111111111111111111111111111111111111111111111"
                rpc_tx_obj = None
                res = raw_input("Would you like the simulation to associate a "
                                "fake txid {} with these payouts? Don't do "
                                "this on production. [y/n] ".format(coin_txid))
                if res != "y":
                    self.logger.info("Exiting")
                    return True
            else:
                # finally run rpc call to payout
                coin_txid, rpc_tx_obj = self.coin_rpc.send_many(user_payout_amounts, self.config['wallet_account'])
        except CoinRPCException:
            new_balance = self.coin_rpc.get_balance(self.config['wallet_account'])
            if new_balance != balance:
                self.logger.error(
                    "RPC error occured and wallet balance changed! Keeping the "
                    "payout entries locked. simplecoin_rpc dump_incomplete can "
                    "show you the details of the locked entries. If you're SURE"
                    "a double payout hasn't occured, use simplecoin_rpc "
                    "reset_all_locked to reset the entries.", exc_info=True)
                return False
            else:
                self.logger.error("RPC error occured and wallet balance didn't "
                                  "change. Unlocking payouts.")
                # Reset all the payouts so we can try again later
                for payout in payouts:
                    payout.locked = False

                self.db.session.commit()
                return False
        else:
            # Success! Now associate the txid and unlock to allow association
            # with remote to occur
            for payout in payouts:
                payout.locked = False
                payout.txid = coin_txid
                payout.paid_time = datetime.datetime.utcnow()

            self.db.session.commit()
            self.logger.info("Updated {:,} (local) Payouts with txid {}"
                             .format(len(payouts), coin_txid))
            return coin_txid, rpc_tx_obj, payouts

    def associate_all(self, simulate=False):
        """
        Looks at all local Payout objects (of the currency_code) that are paid
        and have a transaction id, and attempts to push that transaction ids
        and fees to the SC Payout object
        """
        payouts = (self.db.session.query(Payout).
                   filter_by(associated=False,
                             currency_code=self.config['currency_code']).
                   filter(Payout.txid != None)
                   .all())

        # Build a dict keyed by txid to track payouts.
        txids = {}
        for payout in payouts:
            txids.setdefault(payout.txid, [])
            txids[payout.txid].append(payout)

        # Try to grab the fee for each txid
        tx_fees = {}
        for txid in txids.iterkeys():
            try:
                tx_fees[txid] = self.coin_rpc.get_transaction(txid).fee
            except CoinRPCException as e:
                self.logger.warn('Skipping transaction with id {}, failed '
                                 'looking it up from the {} wallet'
                                 .format(txid, self.config['currency_code']))
                continue

        for txid, payouts in txids.iteritems():
            if simulate:
                self.logger.info("Attempting remote association of {:,} ids "
                                 "with txid {}".format(len(payouts), txid))
            self.associate(txid, payouts, tx_fees[txid], simulate=simulate)

    def associate(self, txid, payouts, tx_fee, simulate=False):
        """
        Attempt to associate Payout objects on SC with a specific transaction ID
        that paid them. Also post the fee incurred by the transaction.
        """
        pids = [p.pid for p in payouts]
        self.logger.info("Trying to associate {:,} payouts with txid {}"
                         .format(len(payouts), txid))

        data = {'coin_txid': txid, 'pids': pids, 'tx_fee': tx_fee,
                'currency': self.config['currency_code']}

        if simulate:
            self.logger.info('We\'re simulating, so don\'t actually post to SC')
            return

        res = self.post('associate_payouts', data=data)
        if res['result']:
            self.logger.info("Received success response from the server.")
            for payout in payouts:
                payout.associated = True
                payout.assoc_time = datetime.datetime.utcnow()
            self.db.session.commit()
            return True
        else:
            self.logger.error("Failed to push association information for {} "
                              "payouts!".format(self.config['currency_code']))
        return False

    def confirm_trans(self, simulate=False):
        """ Grabs the unconfirmed transactions objects from the remote server
        and checks if they're confirmed. Also grabs and pushes the fees for the
        transaction if remote server supports it. """
        self.coin_rpc.poke_rpc()

        res = self.get('api/transaction?__filter_by={{"confirmed":false,"currency":"{}"}}'
                       .format(self.config['currency_code']), signed=False)

        if not res['success']:
            self.logger.error("Failure grabbing unconfirmed transactions: {}".format(res))
            return

        tids = []
        for sc_obj in res['objects']:
            self.logger.debug("Connecting to coinserv to lookup confirms for {}"
                              .format(sc_obj['txid']))
            rpc_tx_obj = self.coin_rpc.get_transaction(sc_obj['txid'])

            if rpc_tx_obj.confirmations > self.config['min_tx_confirms']:
                tids.append(sc_obj['txid'])
                self.logger.info("Confirmed txid {} with {} confirms"
                                 .format(sc_obj['txid'], rpc_tx_obj.confirmations))

        if simulate:
            self.logger.info('We\'re simulating, so don\'t actually post to SC')
            return

        if tids:
            data = {'tids': tids}
            res = self.post('confirm_transactions', data=data)
            if res['result']:
                self.logger.info("Sucessfully confirmed transactions")
                # XXX: Add number print outs
                # XXX: Delete/archive payout row
                return True

            self.logger.error("Failed to push confirmation information")
            return False
        else:
            self.logger.info("No transactions with the minimum confirmations")

    ########################################################################
    # Helpful local data management + analysis methods
    ########################################################################
    def reset_all_locked(self, simulate=False):
        """ Resets all locked payouts """
        payouts = self.db.session.query(Payout).filter_by(locked=True)
        self.logger.info("Resetting {:,} payout ids".format(payouts.count()))
        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            return

        payouts.update({Payout.locked: False})
        self.db.session.commit()

    def init_db(self, simulate=False):
        """ Deletes all data from DB and rebuilds tables. Use carefully... """
        Payout.__table__.drop(self.engine, checkfirst=True)
        Payout.__table__.create(self.engine, checkfirst=True)
        self.db.session.commit()

    def _tabulate(self, title, query, headers=None):
        """ Displays a table of payouts given a query to fetch payouts with, a
        title to label the table, and an optional list of columns to display
        """
        print("@@ {} @@".format(title))
        headers = headers if headers else ["pid", "user", "amount_float", "associated", "locked", "trans_id"]
        data = [p.tabulize(headers) for p in query]
        if data:
            print(tabulate(data, headers=headers, tablefmt="grid"))
        else:
            print("-- Nothing to display --")
        print("")

    def dump_incomplete(self, simulate=False, unpaid_locked=True, paid_unassoc=True, unpaid_unlocked=True):
        """ Prints out a nice display of all incomplete payout records. """
        if unpaid_locked:
            self.unpaid_locked()
        if paid_unassoc:
            self.paid_unassoc()
        if unpaid_unlocked:
            self.unpaid_unlocked()

    def unpaid_locked(self):
        self._tabulate(
            "Unpaid locked {} payouts".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(txid=None, locked=True))

    def paid_unassoc(self):
        self._tabulate(
            "Paid un-associated {} payouts".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(associated=False).filter(Payout.txid != None))

    def unpaid_unlocked(self):
        self._tabulate(
            "{} payouts ready to payout".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(txid=None, locked=False))

    def call(self, command, **kwargs):
        try:
            return getattr(self, command)(**kwargs)
        except Exception:
            self.logger.error("Unhandled exception calling {} with {}"
                              .format(command, kwargs), exc_info=True)
            return False


def entry():
    parser = argparse.ArgumentParser(prog='simplecoin RPC')
    parser.add_argument('-c', '--config', default='config.yml', type=argparse.FileType('r'))
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
    parser.add_argument('-s', '--simulate', action='store_true', default=False)
    subparsers = parser.add_subparsers(title='main subcommands', dest='action')

    subparsers.add_parser('confirm_trans',
                          help='fetches unconfirmed transactions and tries to confirm them')
    subparsers.add_parser('payout', help='pays out all ready payout records')
    subparsers.add_parser('pull_payouts', help='pulls down new payouts that are ready from the server')
    subparsers.add_parser('reset_all_locked', help='resets all locked payouts')
    subparsers.add_parser('dump_incomplete', help='')
    subparsers.add_parser('associate_all', help='')

    args = parser.parse_args()

    global_args = ['log_level', 'action', 'config']
    # subcommand functions shouldn't recieve arguments directed at the
    # global object/ configs
    kwargs = {k: v for k, v in vars(args).iteritems() if k not in global_args}

    config = yaml.load(args.config)
    if args.log_level:
        config['log_level'] = args.log_level
    interface = SCRPCClient(config)
    interface.call(args.action, **kwargs)
