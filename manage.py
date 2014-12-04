import logging
import os
import argparse
import yaml

from cryptokit.rpc_wrapper import CoinRPC
from simplecoin_rpc_client.sc_rpc import SCRPCClient

logger = logging.getLogger('SCRPCClient')
os_root = os.path.abspath(os.path.dirname(__file__) + '/../')


def entry():
    """
    Run a payout/trade functions individually

    Eg.
    python manage.py -c LTC -f get_open_trade_requests

    to run the sc_rpc.py get_open_trade_requests function
    """

    parser = argparse.ArgumentParser(prog='SimpleCoinMulti RPC client manager')
    parser.add_argument('-c', '--config', default='/config.yml', type=argparse.FileType('r'))
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
    parser.add_argument('-s', '--simulate', action='store_true', default=False)

    subparsers = parser.add_subparsers(
        help='Sub-command help',
        choices=['payout', 'trade_request'],
        dest='manager')

    # Setup payout parsers
    payout_parser = subparsers.add_parser(
        'payout',
        help="Performs payout functions",
        choices=['pull_payouts', 'payout', 'confirm_trans',
                 'associate_all', 'reset_all_locked', 'unpaid_locked',
                 'unpaid_unlocked', 'dump_complete', 'dump_incomplete',
                 'init_db'],
        dest='function')
    payout_subparsers = payout_parser.add_subparsers(
        help='Payout Sub-command help',
        dest='function_args')

    # Setup trade request parser
    trade_parser = subparsers.add_parser(
        'trade_request',
        help="Performs trade request functions",
        choices=['get_open_trade_requests', 'close_trade_request',
                 'close_sell_requests'],
        dest='function')
    trade_subparsers = payout_parser.add_subparsers(
        help='Trade Sub-command help',
        dest='function_args')


    # Main payout functions
    payout_parser.add_argument(
        'pull_payouts',
        help="pulls down new payouts that are ready from the server",
        dest="action")
    payout_parser.add_argument(
        'send_payout',
        help="pays out all ready payout records")
    payout_parser.add_parser(
        'associate_all',
        help="Associates all unassociated payouts")
    payout_parser.add_argument(
        'confirm_trans',
        help="fetches unconfirmed transactions and tries to confirm them")

    # Payout helper functions
    payout_parser.add_argument(
        'reset_all_locked',
        help="resets all locked payouts")
    payout_parser.add_argument(
        'unpaid_locked',
        help="Prints out a nice display of all incompleted payout records "
             "which are locked.")
    payout_parser.add_argument(
        'unpaid_unlocked',
        help="Prints out a nice display of all incompleted payout records "
             "which aren't locked.")
    payout_parser.add_argument(
        'dump_complete',
        help="Prints out a nice display of all completed payout records.")
    payout_parser.add_argument(
        'dump_incomplete',
        help="Prints out a nice display of all ncomplete payout records.")
    payout_parser.add_argument(
        'init_db',
        help="Drops & creates the payout table")

    local_associate_locked = payout_subparsers.add_parser(
        'local_associate_locked',
        help="Associate locally locked payouts to an TX id")
    local_associate_locked.add_argument(
        'pid',
        help="Payout id to add txid to")
    local_associate_locked.add_argument(
        'txid',
        help="Transaction id to associate")

    local_associate_all_locked = payout_subparsers.add_parser(
        'local_associate_all_locked',
        help="Associates all locally locked payouts to an TX id")
    local_associate_all_locked.add_argument(
        'txid',
        help="Transaction id to associate")

    # Main trade request functions
    trade_parser.add_argument(
        'get_open_trade_requests',
        help="Pulls down all open trade requests from the server",
        dest="action")

    close_trade_request = trade_subparsers.add_parser(
        'close_trade_request',
        help="Closes a trade request")
    close_trade_request.add_argument(
        'tr_id',
        help="Trade request id to close")
    close_trade_request.add_argument(
        'quantity',
        help="Quantity received from trade(s). BTC for sells. CURR for buys")
    close_trade_request.add_argument(
        'total_fees',
        help="TX fees & exchange fees associated with making trades. "
             "BTC for sells. CURR for buys")

    close_sell_requests = trade_parser.add_parser(
        'close_sell_requests',
        help="Associates all unassociated payouts")
    close_sell_requests.add_argument(
        'tr_ids',
        nargs='+',
        help="List (space separated) of sell request ids to close")
    close_sell_requests.add_argument(
        'btc_quantity',
        help="BTC Quantity received from sells.")
    close_sell_requests.add_argument(
        'btc_fees',
        help="TX fees & exchange fees associated with making trades, in BTC")

    args = parser.parse_args()
    config = yaml.load(args.config)

    # Args override config
    if args.log_level:
        config['log_level'] = args.log_level

    # Setup our CoinRPCs + SCRPCClients
    coin_rpc = {}
    sc_rpc = {}
    for curr_cfg in config['currencies']:

        if not curr_cfg['enabled']:
            continue

        cc = curr_cfg['currency_code']
        coin_rpc[cc] = CoinRPC(curr_cfg, logger=logger)

        curr_cfg.update(config['sc_rpc_client'])
        sc_rpc[cc] = SCRPCClient(curr_cfg, coin_rpc[cc], logger=logger)

    if not sc_rpc:
        logger.error("At least one currency must be configured! Exiting...")
        exit(0)

    manager = args.command
    function_args = args.function_args

    if manager == 'payout':
        # run the action
        for currency, sc_rpc in sc_rpc.iteritems():
            function = getattr(sc_rpc[currency].payout_manager, args.function)
            function(*function_args)
    elif manager == 'trade_request':
        currency = sc_rpc.itervalues()[0]
        function = getattr(sc_rpc[currency].payout_manager, args.function)
        function(*function_args)