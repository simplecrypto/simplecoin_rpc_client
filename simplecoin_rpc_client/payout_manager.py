import datetime
import sqlalchemy as sa

from tabulate import tabulate
from urllib3.exceptions import ConnectionError
from sqlalchemy.ext.declarative import declarative_base
from cryptokit.rpc import CoinRPCException
from cryptokit.base58 import get_bcaddress_version


base = declarative_base()


class Payout(base):
    """ Our single table in the sqlite database. Handles tracking the status of
    payouts and keeps track of tasks that needs to be retried, etc. """
    __tablename__ = "payouts"
    id = sa.Column(sa.Integer, primary_key=True)
    pid = sa.Column(sa.String, unique=True, nullable=False)
    user = sa.Column(sa.String, nullable=False)
    address = sa.Column(sa.String, nullable=False)
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


class PayoutManager(object):

    def __init__(self, sc_rpc):
        self.sc_rpc = sc_rpc
        self.logger = sc_rpc.logger
        self.config = sc_rpc.config
        self.db = sc_rpc.db
        self.coin_rpc = sc_rpc.coin_rpc

    ########################################################################
    # RPC Client Payout methods
    ########################################################################
    def pull_payouts(self, simulate=False):
        """ Gets all the unpaid payouts from the server """

        if simulate:
            self.logger.info('#'*20 + ' Simulation mode ' + '#'*20)

        try:
            payouts = self.sc_rpc.post(
                'get_payouts',
                data={'currency': self.config['currency_code']}
            )['pids']
        except ConnectionError:
            self.logger.warn('Unable to connect to SC!', exc_info=True)
            return

        if not payouts:
            self.logger.info("No {} payouts to process.."
                             .format(self.config['currency_code']))
            return

        repeat = 0
        new = 0
        invalid = 0
        for user, address, amount, pid in payouts:
            # Check address is valid
            if not get_bcaddress_version(address) in self.config['valid_address_versions']:
                self.logger.warn("Ignoring payout {} due to invalid address. "
                                 "{} address did not match a valid version {}"
                                 .format((amount, pid),
                                         self.config['currency_code'],
                                         self.config['valid_address_versions']))
                invalid += 1
                continue
            # Check payout doesn't already exist
            if self.db.session.query(Payout).filter_by(pid=pid).first():
                self.logger.debug("Ignoring payout {} because it already exists"
                                  " locally".format((user, address, amount, pid)))
                repeat += 1
                continue
            # Create local payout obj
            p = Payout(pid=pid, user=user, address=address, amount=amount,
                       currency_code=self.config['currency_code'],
                       pull_time=datetime.datetime.utcnow())
            new += 1

            if not simulate:
                self.db.session.add(p)

        self.db.session.commit()

        self.logger.info("Inserted {:,} new {} payouts and skipped {:,} old "
                         "payouts from the server. {:,} payouts with invalid addresses."
                         .format(new, self.config['currency_code'], repeat, invalid))
        return True

    def send_payout(self, simulate=False):
        """ Collects all the unpaid payout ids (for the configured currency)
        and pays them out """
        if simulate:
            self.logger.info('#'*20 + ' Simulation mode ' + '#'*20)

        try:
            self.coin_rpc.poke_rpc()
        except CoinRPCException as e:
            self.logger.warn(
                "Error occured while trying to get info from the {} RPC. Got "
                "{}".format(self.config['currency_code'], e))
            return False

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

        # track the total payouts to each address
        address_payout_amounts = {}
        pids = {}
        for payout in payouts:
            address_payout_amounts.setdefault(payout.address, 0.0)
            address_payout_amounts[payout.address] += float(payout.amount)
            pids.setdefault(payout.address, [])
            pids[payout.address].append(payout.pid)

            # We'll lock the payout before continuing in case of a failure in
            # between paying out and recording that payout action
            payout.locked = True
            payout.lock_time = datetime.datetime.utcnow()

        for address, amount in address_payout_amounts.items():
            # Convert amount from STR and coerce to a payable value.
            # Note that we're not trying to validate the amount here, all
            # validation should be handled server side.
            amount = round(float(amount), 8)

            if amount < self.config['minimum_tx_output']:
                # We're unable to pay, so undo the changes from the last loop
                self.logger.warn('Removing {} with payout amount of {} (which '
                                 'is lower than network output min of {}) from '
                                 'the {} payout dictionary'
                                 .format(address, amount,
                                         self.config['minimum_tx_output'],
                                         self.config['currency_code']))

                address_payout_amounts.pop(address)
                pids[address] = []
                for payout in payouts:
                    if payout.address == address:
                        payout.locked = False
                        payout.lock_time = None
            else:
                address_payout_amounts[address] = amount

        total_out = sum(address_payout_amounts.values())
        balance = self.coin_rpc.get_balance(self.coin_rpc.coinserv['account'])
        self.logger.info("Account balance for {} account \'{}\': {:,}"
                         .format(self.config['currency_code'],
                                 self.coin_rpc.coinserv['account'], balance))
        self.logger.info("Total to be paid {:,}".format(total_out))

        if balance < total_out:
            self.logger.error("Payout wallet is out of funds!")
            self.db.session.rollback()
            # XXX: Add an email call here
            return False

        if total_out == 0:
            self.logger.info("Paying out 0 funds! Aborting...")
            self.db.session.rollback()
            return True

        if not simulate:
            self.db.session.commit()
        else:
            self.db.session.rollback()

        def format_pids(pids):
            lst = ", ".join(pids[:9])
            if len(pids) > 9:
                return lst + "... ({} more)".format(len(pids) - 8)
            return lst
        summary = [(str(address), amount, str(format_pids(upids))) for
                   (address, amount), upids in zip(address_payout_amounts.iteritems(), pids.itervalues())]

        self.logger.info(
            "Address payment summary\n" + tabulate(summary, headers=["Address", "Total", "Pids"], tablefmt="grid"))

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
                coin_txid, rpc_tx_obj = self.coin_rpc.send_many(
                    self.coin_rpc.coinserv['account'], address_payout_amounts)
        except CoinRPCException as e:
            self.logger.warn(e)
            new_balance = self.coin_rpc.get_balance(self.coin_rpc.coinserv['account'])
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
            payout_addrs = [address for address in address_payout_amounts.iterkeys()]
            finalized_payouts = []
            for payout in payouts:
                if payout.address in payout_addrs:
                    payout.locked = False
                    payout.txid = coin_txid
                    payout.paid_time = datetime.datetime.utcnow()
                    finalized_payouts.append(payout)

            self.db.session.commit()
            self.logger.info("Updated {:,} (local) Payouts with txid {}"
                             .format(len(finalized_payouts), coin_txid))
            return coin_txid, rpc_tx_obj, finalized_payouts

    def associate_all(self, simulate=False):
        """
        Looks at all local Payout objects (of the currency_code) that are paid
        and have a transaction id, and attempts to push that transaction ids
        and fees to the SC Payout object
        """
        if simulate:
            self.logger.info('#'*20 + ' Simulation mode ' + '#'*20)

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

        data = {'coin_txid': txid, 'pids': pids, 'tx_fee': float(tx_fee),
                'currency': self.config['currency_code']}

        if simulate:
            self.logger.info('We\'re simulating, so don\'t actually post to SC')
            return

        res = self.sc_rpc.post('associate_payouts', data=data)
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
        self.logger.info("Attempting to grab unconfirmed {} transactions from "
                         "SC, poking the RPC...".format(self.config['currency_code']))
        try:
            self.coin_rpc.poke_rpc()
        except CoinRPCException as e:
            self.logger.warn(
                "Error occured while trying to get info from the {} RPC. Got "
                "{}".format(self.config['currency_code'], e))
            return False

        res = self.sc_rpc.get(
            'api/transaction?__filter_by={{"confirmed":false,"currency":"{}"}}'
            .format(self.config['currency_code']), signed=False)

        if not res['success']:
            self.logger.error("Failure grabbing unconfirmed transactions: {}"
                              .format(res))
            return

        if not res['objects']:
            self.logger.info("No transactions were returned to confirm...exiting.")
            return

        tids = []
        for sc_obj in res['objects']:
            self.logger.debug("Connecting to coinserv to lookup confirms for {}"
                              .format(sc_obj['txid']))
            rpc_tx_obj = self.coin_rpc.get_transaction(sc_obj['txid'])

            if rpc_tx_obj.confirmations > self.config['min_confirms']:
                tids.append(sc_obj['txid'])
                self.logger.info(
                    "Confirmed txid {} with {} confirms"
                    .format(sc_obj['txid'], rpc_tx_obj.confirmations))
            else:
                self.logger.info(
                    "TX {} not yet confirmed. {}/{} confirms"
                    .format(sc_obj['txid'], rpc_tx_obj.confirmations,
                            self.config['min_confirms']))

        if simulate:
            self.logger.info("We're simulating, so don't actually post to SC")
            return

        if tids:
            data = {'tids': tids}
            res = self.sc_rpc.post('confirm_transactions', data=data)
            if res['result']:
                self.logger.info("Sucessfully confirmed transactions")
                # XXX: Add number print outs
                # XXX: Delete/archive payout row
                return True

            self.logger.error("Failed to push confirmation information")
            return False

    ########################################################################
    # Helpful local data management + analysis methods
    ########################################################################
    def init_db(self, simulate=False):
        """ Deletes all data from DB and rebuilds tables. Use carefully... """
        Payout.__table__.drop(self.sc_rpc.engine, checkfirst=True)
        Payout.__table__.create(self.sc_rpc.engine, checkfirst=True)
        self.db.session.commit()

    def reset_all_locked(self, simulate=False):
        """ Resets all locked payouts """
        payouts = self.db.session.query(Payout).filter_by(locked=True)
        self.logger.info("Resetting {:,} payout ids".format(payouts.count()))
        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            return

        payouts.update({Payout.locked: False})
        self.db.session.commit()

    def local_associate_locked(self, pid, tx_id, simulate=False):
        """
        Locally associates a payout, with which is both unpaid and
        locked, with a TXID.
        """
        payout = (self.db.session.query(Payout)
                  .filter_by(txid=None, locked=True, id=pid).all())
        self.logger.info("Associating payout id {} with TX ID {}"
                         .format(payout.id, tx_id))
        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            return

        payout.txid = tx_id

        self.db.session.commit()
        return True

    def local_associate_all_locked(self, tx_id, simulate=False):
        """
        Locally associates payouts for this _currency_ which are both unpaid and
        locked with a TXID

        If you want to locally associate an individual payout ID with a TX use
        local_associate_locked()

        You'll want to use this function if a payout went out and you have the
        txid, but for whatever reason it didn't get saved/updated in the local
        DB. After you've done this you'll still need to run the functions to
        associate everything on the remote server after.
        """
        payouts = (self.db.session.query(Payout)
                   .filter_by(txid=None, locked=True,
                              currency_code=self.config['currency_code'])
                   .all())
        self.logger.info("Associating {:,} payout ids with TX ID {}"
                         .format(len(payouts), tx_id))
        if simulate:
            self.logger.info("Just kidding, we're simulating... Exit.")
            return

        for payout in payouts:
            payout.txid = tx_id

        self.db.session.commit()
        return True

    def _tabulate(self, title, query, headers=None, data=None):
        """ Displays a table of payouts given a query to fetch payouts with, a
        title to label the table, and an optional list of columns to display
        """
        print("@@ {} @@".format(title))
        headers = headers if headers else ["pid", "user", "address", "amount_float", "associated", "locked", "trans_id"]
        data = [p.tabulize(headers) for p in query]
        if data:
            print(tabulate(data, headers=headers, tablefmt="grid"))
        else:
            print("-- Nothing to display --")
        print("")

    def dump_incomplete(self, unpaid_locked=True, paid_unassoc=True, unpaid_unlocked=True, simulate=False):
        """ Prints out a nice display of all incomplete payout records. """
        if unpaid_locked:
            self.unpaid_locked()
        if paid_unassoc:
            self.paid_unassoc()
        if unpaid_unlocked:
            self.unpaid_unlocked()

    def unpaid_locked(self, simulate=False):
        self._tabulate(
            "Unpaid locked {} payouts".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(txid=None, locked=True).all())

    def paid_unassoc(self, simulate=False):
        self._tabulate(
            "Paid un-associated {} payouts".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(associated=False).filter(Payout.txid != None).all())

    def unpaid_unlocked(self, simulate=False):
        self._tabulate(
            "{} payouts ready to payout".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(txid=None, locked=False).all())

    def dump_complete(self, simulate=False):
        """ Prints out a nice display of all completed payout records. """
        self._tabulate(
            "Paid + associated {} payouts".format(self.config['currency_code']),
            self.db.session.query(Payout).filter_by(associated=True).filter(Payout.txid != None).all())

    def call(self, command, **kwargs):
        try:
            return getattr(self, command)(**kwargs)
        except Exception:
            self.logger.error("Unhandled exception calling {} with {}"
                              .format(command, kwargs), exc_info=True)
            return False