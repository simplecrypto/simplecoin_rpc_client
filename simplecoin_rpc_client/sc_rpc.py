import logging
import sys
import os
import datetime
import requests
import sqlalchemy as sa

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from urlparse import urljoin
from itsdangerous import TimedSerializer, BadData
from simplecoin_rpc_client.payout_manager import PayoutManager
from simplecoin_rpc_client.trade_manager import TradeManager


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


class SCRPCException(Exception):
    pass


class SCRPCClient(object):
    def _set_config(self, **kwargs):
        # A fast way to set defaults for the kwargs then set them as attributes
        base = os.path.abspath(os.path.dirname(__file__) + '/../')
        self.config = dict(max_age=10,
                           logger_name="sc_rpc_client",
                           log_level="INFO",
                           database_path=base + '/rpc_',
                           log_path=base + '/sc_rpc.log',
                           min_confirms=12,
                           minimum_tx_output=0.00000001)
        self.config.update(kwargs)

        required_conf = ['valid_address_versions', 'currency_code',
                         'rpc_signature', 'rpc_url']
        error = False
        for req in required_conf:
            if req not in self.config:
                print("{} is a required configuration variable".format(req))
                error = True

        if error:
            raise SCRPCException('Errors occurred while configuring RPCClient obj')

        # Kinda sloppy, but it works
        self.config['database_path'] += self.config['currency_code'] + '.sqlite'

    def __init__(self, config, CoinRPC, logger=None):

        if not config:
            raise SCRPCException('Invalid configuration file')
        self._set_config(**config)

        # Setup CoinRPC
        self.coin_rpc = CoinRPC

        # Setup Payout Manager
        self.payout_manager = PayoutManager(self)

        # Setup Trade Manager
        self.trade_manager = TradeManager(self)

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
        if logger:
            self.logger = logger
        else:
            logging.Formatter.converter = datetime.time.gmtime
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