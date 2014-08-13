import logging
import datetime
import time
import urllib3
import requests
import sqlalchemy
import setproctitle
import decorator
import argparse
import json

from time import sleep
from apscheduler.scheduler import Scheduler

logger = logging.getLogger('apscheduler.scheduler')


@decorator.decorator
def crontab(func, *args, **kwargs):
    """ Handles rolling back SQLAlchemy exceptions to prevent breaking the
    connection for the whole scheduler. Also records timing information into
    the cache """

    res = None
    try:
        res = func(*args, **kwargs)
    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error("SQLAlchemyError occurred, rolling back: {}".format(e))
        db.session.rollback()
    except Exception:
        logger.error("Unhandled exception in {}".format(func.__name__),
                     exc_info=True)

    return res


@crontab
def run_payouts():
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='simplecoin task scheduler')
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'],
                        default='INFO')
    args = parser.parse_args()

    root = logging.getLogger()
    hdlr = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s] %(message)s')
    hdlr.setFormatter(formatter)
    root.addHandler(hdlr)
    root.setLevel(getattr(logging, args.log_level))

    sched = Scheduler(standalone=True)
    logger.info("=" * 80)
    logger.info("SimpleCoin cron scheduler starting up...")
    setproctitle.setproctitle("simplecoin_scheduler")

    # All these tasks actually change the database, and shouldn't
    # be run by the staging server
    sched.add_cron_job(run_payouts, minute='0,5,10,15,20,25,30,35,40,45,50,55', second=30)

    sched.start()
