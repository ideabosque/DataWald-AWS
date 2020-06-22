from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
from decimal import Decimal
import traceback, json

class BCAgency(FrontEnd, BackOffice):
    def __init__(self, setting=None, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        if feConn is not None:
            self.bc = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.bc = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

    def feOrdersFt(self, cutdt):
        rawOrders = self.bc.getOrders(cutdt)
        orders = []
        for _order in rawOrders:
            shippingAddresses = self.bc.getShippingAddresses(_order["id"])
            order = {
                "addresses": {
                    "billto": {
                        # "address": _order["billing_address"]["street_1"],
                        "city": _order["billing_address"]["city"],
                        # "company": _order["billing_address"].pop("company", "####"),
                        "postcode": _order["billing_address"]["zip"],
                        "region": _order["billing_address"]["state"]
                    },
                    "shipto": {
                        # "address": shippingAddresses[0]["street_1"],
                        "city": shippingAddresses[0]["city"],
                        # "company": shippingAddresses[0].pop("company", "####"),
                        "postcode": shippingAddresses[0]["zip"],
                        "region": shippingAddresses[0]["state"]
                    }
                },
                "fe_order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # datetime.strptime(_order["date_created"], "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d %H:%M:%S"),
                "fe_order_id": str(_order["id"]),
                "fe_order_status": _order["status"],
                "fe_order_update_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # datetime.strptime(_order["date_modified"], "%a, %d %b %Y %H:%M:%S %z").strftime("%Y-%m-%d %H:%M:%S"),
                # "ship_method": shippingAddresses[0].pop("shipping_method", "####"),
                "items": [
                    {
                        "qty": product["quantity"],
                        "sku": product["sku"]
                    } for product in self.bc.getOrderProducts(_order["id"])
                ]
            }
            orders.append(order)

        return (orders, rawOrders)

    def feOrdersExtFt(self, orders, rawOrders):
        pass
