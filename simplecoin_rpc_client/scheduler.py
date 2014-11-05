import logging
import os
import decorator
import sqlalchemy
import setproctitle
import argparse
import yaml

from apscheduler.scheduler import Scheduler
from cryptokit.rpc_wrapper import CoinRPC
from simplecoin_rpc_client.sc_rpc import SCRPCClient

logger = logging.getLogger('apscheduler.scheduler')
os_root = os.path.abspath(os.path.dirname(__file__) + '/../')


@decorator.decorator
def crontab(func, *args, **kwargs):
    """ Handles rolling back SQLAlchemy exceptions to prevent breaking the
    connection for the whole scheduler. Also records timing information into
    the cache """
    self = args[0]

    res = None
    try:
        res = func(*args, **kwargs)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error("SQLAlchemyError occurred, rolling back: {}".format(e))
        self.db.session.rollback()
    except Exception:
        self.logger.error("Unhandled exception in {}".format(func.__name__),
                          exc_info=True)

    return res


class PayoutManager(object):

    def __init__(self, logger, sc_rpc, coin_rpc):
        self.logger = logger
        self.sc_rpc = sc_rpc
        self.coin_rpc = coin_rpc

    @crontab
    def pull_payouts(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.pull_payouts()

    @crontab
    def send_payout(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():

            # Try to pay out known payouts
            result = sc_rpc.send_payout()
            if isinstance(result, bool):
                continue
            else:
                coin_txid, tx, payouts = result
                sc_rpc.associate_all()

            # Push completed payouts to SC
            sc_rpc.associate(coin_txid, payouts, tx.fee)

    @crontab
    def associate_all_payouts(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.associate_all()

    @crontab
    def confirm_payouts(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.confirm_trans()

    @crontab
    def init_db(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.init_db()

    @crontab
    def dump_incomplete(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.dump_incomplete()

    @crontab
    def dump_complete(self):
        for currency, sc_rpc in self.sc_rpc.iteritems():
            sc_rpc.dump_complete()


def entry():
    parser = argparse.ArgumentParser(prog='simplecoin rpc client scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    parser.add_argument('-cl', '--config-location',
                        default='/config.yml')
    args = parser.parse_args()

    # Setup logging
    root = logging.getLogger()
    hdlr = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
    hdlr.setFormatter(formatter)
    root.addHandler(hdlr)
    root.setLevel(getattr(logging, args.log_level))

    # Setup yaml configs
    # =========================================================================
    cfg = yaml.load(open(os_root + args.config_location))

    # Setup our CoinRPCs + SCRPCClients
    coin_rpc = {}
    sc_rpc = {}
    for curr_cfg in cfg['currencies']:

        if not curr_cfg['enabled']:
            continue

        cc = curr_cfg['currency_code']
        coin_rpc[cc] = CoinRPC(curr_cfg, logger=logger)

        curr_cfg.update(cfg['sc_rpc_client'])
        sc_rpc[cc] = SCRPCClient(curr_cfg, coin_rpc[cc], logger=logger)

    pm = PayoutManager(logger, sc_rpc, coin_rpc)

    sched = Scheduler(standalone=True)
    logger.info("=" * 80)
    logger.info("SimpleCoin cron scheduler starting up...")
    setproctitle.setproctitle("simplecoin_scheduler")

    # All these tasks actually change the database, and shouldn't
    # be run by the staging server
    sched.add_cron_job(pm.pull_payouts, minute='*/1')
    sched.add_cron_job(pm.send_payout, hour='23')
    sched.add_cron_job(pm.associate_all_payouts, hour='0')
    sched.add_cron_job(pm.confirm_payouts, hour='1')


    sched.start()

if __name__ == "__main__":
    entry()
