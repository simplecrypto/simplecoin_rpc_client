import logging
import os
import argparse
import sys
import yaml

from cryptokit.rpc_wrapper import CoinRPC
from simplecoin_rpc_client.sc_rpc import SCRPCClient

os_root = os.path.abspath(os.path.dirname(__file__) + '/../')


def entry():
    """
    Run a payout/trade functions individually

    Eg.
    python manage.py -c LTC -f get_open_trade_requests

    to run the sc_rpc.py get_open_trade_requests function
    """
    parser = argparse.ArgumentParser(prog='SimpleCoinMulti RPC client manager')
    parser.add_argument('-c', '--config', default='config.yml', type=argparse.FileType('r'))
    parser.add_argument('-l', '--log-level',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])
    parser.add_argument('-s', '--simulate', action='store_true', default=False)

    subparsers = parser.add_subparsers(
        help='Sub-command help',
        dest='manager')

    # Setup payout parsers
    payout_parser = subparsers.add_parser(
        'payout_manager',
        help="Performs payout functions")

    # Setup trade request parser
    trade_parser = subparsers.add_parser(
        'trade_manager',
        help="Performs trade request functions")

    # Main payout functions
    payout_parser.add_argument(
        '-f', '--function',
        choices=['pull_payouts', 'payout', 'confirm_trans',
                 'associate_all', 'reset_all_locked', 'unpaid_locked',
                 'unpaid_unlocked', 'dump_complete', 'dump_incomplete',
                 'local_associate_locked', 'local_associate_all_locked',
                 'init_db'],
        help="Payout function to run")
    payout_parser.add_argument(
        '-a', '--function-args',
        nargs='*',
        help="Arguments to pass the function")

    # Main trade request functions
    trade_parser.add_argument(
        '-f', '--function',
        choices=['get_open_trade_requests', 'close_trade_request',
                 'close_sell_requests', 'close_buy_requests'],
        help="Trade request function to run")
    trade_parser.add_argument(
        '-a', '--function-args',
        nargs='*',
        help="Arguments to pass the function")

    args = parser.parse_args()
    config = yaml.load(args.config)

    # Args override config
    if args.log_level:
        config['log_level'] = args.log_level

    # Setup logging
    logger = logging.getLogger()
    logger.setLevel(config['log_level'])
    log_format = logging.Formatter('%(asctime)s [%(name)s] %(funcName)s [%(levelname)s] %(message)s')
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(log_format)
    logger.addHandler(handler)

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

    manager = args.manager
    function_args = args.function_args
    simulate = args.simulate

    if manager == 'payout_manager':
        for currency in sc_rpc.iterkeys():
            function = getattr(sc_rpc[currency].payout_manager, args.function)
            if function_args:
                function(*function_args, simulate=simulate)
            else:
                function(simulate=simulate)

    elif manager == 'trade_manager':
        currency = config['currencies'][0]['currency_code']
        function = getattr(sc_rpc[currency].trade_manager, args.function)
        if function_args:
            function(*function_args, simulate=simulate)
        else:
            function(simulate=simulate)

if __name__ == "__main__":
    entry()