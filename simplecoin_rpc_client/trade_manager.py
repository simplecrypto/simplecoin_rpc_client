from decimal import Decimal
from pprint import pformat
from tabulate import tabulate
from urllib3.exceptions import ConnectionError


class TradeManager(object):

    def __init__(self, sc_rpc):
        self.sc_rpc = sc_rpc
        self.logger = sc_rpc.logger
        self.config = sc_rpc.config
        self.db = sc_rpc.db
        self.coin_rpc = sc_rpc.coin_rpc

    def get_open_trade_requests(self):
        """
        Grabs the open trade requests from the server and prints off
        info about them
        """

        try:
            trs = self.sc_rpc.post('get_trade_requests')['trs']
        except ConnectionError:
            self.logger.warn('Unable to connect to SC!', exc_info=True)
            return

        if not trs:
            self.logger.info("No trade requests returned from SC..."
                             .format(self.config['currency_code']))

        # basic checking of input
        try:
            for tr_id, currency, quantity, type in trs:
                assert isinstance(tr_id, int)
                assert isinstance(currency, basestring)
                assert isinstance(quantity, float)
                assert isinstance(type, basestring)
                assert type == 'buy' or 'sell'
        except AssertionError:
            self.logger.warn("Invalid TR format returned from RPC call "
                             "get_trade_requests.", exc_info=True)
            return

        brs = []
        srs = []
        for tr_id, currency, quantity, type in trs[:]:
            tr = [tr_id, currency, quantity, type]

            # remove trs not for this currency
            if currency != self.config['currency_code']:
                continue
            elif type == 'sell':
                srs.append(tr)
            elif type == 'buy':
                brs.append(tr)

        self.logger.info("Got {} {} sell requests from SC"
                         .format(len(srs), self.config['currency_code']))
        self.logger.info("Got {} {} buy requests from SC"
                         .format(len(brs), self.config['currency_code']))

        # Print
        headers = ['tr_id', 'currency', 'quantity', 'type']
        print("@@ Open {} sell requests @@".format(self.config['currency_code']))
        print(tabulate(srs, headers=headers, tablefmt="grid"))
        print("@@ Open {} buy requests @@".format(self.config['currency_code']))
        print(tabulate(brs, headers=headers, tablefmt="grid"))
        return srs, brs

    def close_trade_request(self, tr_id, quantity, total_fees, simulate=False):

        if simulate:
            self.logger.info('#'*20 + ' Simulation mode ' + '#'*20)

        completed_trs = {tr_id: {'status': 6,
                                 'quantity': str(quantity),
                                 'fees': str(total_fees)}}

        if not simulate:
            # Post the dictionary
            response = self.sc_rpc.post(
                'update_trade_requests',
                data={'update': True, 'trs': completed_trs}
            )

            if 'success' in response:
                self.logger.info(
                    "Successfully posted {} updated trade requests to SC!"
                    .format(len(completed_trs)))
            else:
                self.logger.warn(
                    "Failed posting request updates! Attempted to post the "
                    "following dictionary: {}".format(pformat(completed_trs)))
        else:
            self.logger.info(
                "Simulating - but would have posted the following dictionary: "
                "{}".format(pformat(completed_trs)))

    def close_sell_requests(self, tr_ids, btc_quantity, btc_fees, simulate=False):

        btc_quantity = Decimal(btc_quantity)
        btc_fees = Decimal(btc_fees)

        if simulate:
            self.logger.info('#'*20 + ' Simulation mode ' + '#'*20)

        # Get values
        srs, _ = self.get_open_trade_requests()

        sr_ids = []
        for sr_id, currency, quantity, type in srs:
            sr_ids.append(sr_id)

        # Check all trade request ids are 'open' on server
        for tr_id in tr_ids:
            if tr_id not in sr_ids:
                self.logger.warning(
                    "Trade request id {} was not found in the open sell "
                    "requests! Aborting...")
                return

        # Prune the list, do some checking + build a total quantity
        total_quant = 0
        for sr_id, currency, quantity, type in srs[:]:
            if sr_id not in tr_ids:
                srs.remove([sr_id, currency, quantity, type])
            else:
                assert type == 'sell'
                total_quant += Decimal(quantity)

        # Avg
        avg_price = btc_quantity / total_quant
        self.logger.info("Computed average price of {} BTC for all Sell "
                         "Requests".format(avg_price))

        # build dictionary to post
        completed_srs = {}
        for sr_id, currency, quantity, type in srs[:]:
            completed_srs.setdefault(sr_id, {})

            self.logger.info("#"*40)
            self.logger.info("Computing values for SR ID #{} ({} {})"
                             .format(sr_id, quantity, currency))

            sr_perc = Decimal(quantity) / total_quant
            sr_btc = sr_perc * btc_quantity
            sr_fees = sr_perc * btc_fees
            sr_avg_price = sr_btc / Decimal(quantity)

            self.logger.info("This SR is {}% of the total"
                             .format(sr_perc * 100))
            self.logger.info("{} total btc".format(sr_btc))
            self.logger.info("{} btc in fees".format(sr_fees))
            self.logger.info("Avg price: {}".format(sr_avg_price))

            completed_srs[sr_id] = {'status': 6,
                                    'quantity': str(sr_btc),
                                    'fees': str(sr_fees)}
        self.logger.info("#"*40)
        self.logger.info("Preparing to post the following values to server: \n "
                         "{}".format(pformat(completed_srs)))

        res = raw_input("Does this look correct? [y/n] ")
        if res != "y":
            return

        if not simulate:
            # Post the dictionary
            response = self.sc_rpc.post(
                'update_trade_requests',
                data={'update': True, 'trs': completed_srs}
            )

            if 'success' in response:
                self.logger.info(
                    "Successfully posted {} updated trade requests to SC!"
                    .format(len(completed_srs)))
            else:
                self.logger.warn(
                    "Failed posting request updates! Attempted to post the "
                    "following response: {}".format(pformat(response)))
        else:
            self.logger.info("Simulating - not posting to server!")
