import logging
import os
import argparse
import yaml

from cryptokit.rpc_wrapper import CoinRPC
from simplecoin_rpc_client.sc_rpc import SCRPCClient

logger = logging.getLogger('apscheduler.scheduler')
os_root = os.path.abspath(os.path.dirname(__file__) + '/../')


def entry():
    """
    Run a sc_rpc.py function individually

    Eg.
    python manage.py -c LTC -f get_open_trade_requests

    to run the sc_rpc.py get_open_trade_requests function
    """
    parser = argparse.ArgumentParser(prog='simplecoin rpc client manager')

    # managed function args
    parser.add_argument('-c', '--currencycode', required=True)
    parser.add_argument('-f', '--function', required=True)
    parser.add_argument('-a', '--args', nargs='+')

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

    function_args = []
    if hasattr(args, 'args'):
        function_args = args.args or []

    function = getattr(sc_rpc[args.currencycode], args.function)
    function(*function_args)


if __name__ == "__main__":
    entry()
