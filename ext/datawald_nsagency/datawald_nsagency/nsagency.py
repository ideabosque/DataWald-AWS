from datawald_backoffice import BackOffice
from datawald_frontend import FrontEnd
from datetime import datetime, timedelta
from decimal import Decimal
from pytz import timezone
import traceback, json
from .txmap import TXMAP


class NSAgency(FrontEnd, BackOffice):
    def __init__(self, setting={}, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        self.map = TXMAP
        if setting is not None:
            self.map = setting.get("TXMAP") if setting.get("TXMAP") is not None else TXMAP
        self.endDt = datetime.now(tz=timezone(self.setting.get("TIMEZONE", "UTC"))).strftime("%Y-%m-%d %H:%M:%S")
        self.itemDetail = setting.get("ITEMDETAIL", False)
        self.join = setting.get("JOIN", {'base': [], 'lines': []})
        self.limit = int(setting.get("LIMIT", 100))
        self.hours = float(setting.get("HOURS", 0))
        if feConn is not None:
            feConn.timezone = self.setting.get("TIMEZONE", "UTC")
            self.ns = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            boConn.timezone = self.setting.get("TIMEZONE", "UTC")
            self.ns = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

    @property
    def paymentMethods(self):
        if "PAYMENTMETHODS" in self.setting.keys():
            return self.setting["PAYMENTMETHODS"]
        else:
            return {
                "####": "Net Terms"
            }

    @property
    def shipMethods(self):
        if "SHIPMETHODS" in self.setting.keys():
            return self.setting["SHIPMETHODS"]
        else:
            return {
                "####": "Will Call"
            }

    @property
    def countries(self):
        if "COUNTRIES" in self.setting.keys():
            return self.setting["COUNTRIES"]
        else:
            return {
                "US": "_unitedStates"
            }

    def _getRecords(self, params, funct):
        try:
            current = datetime.now(tz=timezone(self.setting.get("TIMEZONE", "UTC")))
            while True:
                self.logger.info(params)
                records = funct(**params)
                end =  datetime.strptime(params.get("cutDt"), "%Y-%m-%d %H:%M:%S") + timedelta(hours=params.get('hours'))
                end = end.replace(tzinfo=timezone(self.setting.get("TIMEZONE", "UTC")))
                if self.hours == 0.0:
                    return records
                elif len(records) >= 1 or end >= current:
                    return records
                else:
                    params["hours"] = params["hours"] + self.hours
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def _getTerm(self, paymentMethod):
        terms = self.setting["TERMS"] if "TERMS" in self.setting.keys() else {
            "Net 15": ["Net Terms"],
            "Credit Card": ["Visa"]
        }

        term = None
        for k, v in terms.items():
            if paymentMethod in v:
                term = k
                break
        return term

    def setRawData(self, **params):
        endDt = params.pop("end_dt", None)
        limit = params.pop("limit", None)
        if endDt is not None:
            self.endDt = endDt
        if limit is not None:
            self.limit = int(limit)

    def transformData(self, record, metadatas):
        return super(NSAgency, self).transformData(record, metadatas, getCustValue=self.getCustomFieldValue)

    def getCustomFieldValue(self, record, scriptId):
        value = None
        if record["customFieldList"] is None:
            return value
        _customFields = list(
            filter(
                lambda customField: (customField["scriptId"] == scriptId.replace("@", "")), 
                record["customFieldList"]["customField"]
            )
        )
        if len(_customFields) == 1:
            value = _customFields[0]["value"]
        return value

    def boPurchaseOrdersFt(self, frontend, cutDt=None):
        try:
            params = {
                "vendorId": self.setting.get('VENDORID'),
                "cutDt": cutDt,
                "endDt": self.endDt,
                "itemDetail": self.itemDetail,
                "join": self.join,
                "limit": self.limit,
                "hours": self.hours
            }
            rawPurchaseOrders = self._getRecords(params, funct=self.ns.getPurchaseOrders)
            purchaseOrders = []
            for rawPurchaseOrder in rawPurchaseOrders:
                purchaseOrder = {}
                purchaseOrder["bo_po_num"] = rawPurchaseOrder.tranId
                purchaseOrder["frontend"] = frontend
                purchaseOrder["data"] = self.transformData(rawPurchaseOrder, self.map["purchaseOrder"])
                purchaseOrder["create_dt"] = rawPurchaseOrder.createdDate.strftime('%Y-%m-%d %H:%M:%S')
                purchaseOrder["update_dt"] = rawPurchaseOrder.lastModifiedDate.strftime('%Y-%m-%d %H:%M:%S')
                purchaseOrders.append(purchaseOrder)
            return (purchaseOrders, rawPurchaseOrders)
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def boItemReceiptFt(self, itemReceipt):
        return self.transformData(itemReceipt, self.map["itemReceipt"])

    def insertItemReceiptsFt(self, newItemReceipts):
        return self.ns.insertItemReceipts(newItemReceipts)

    def setInventory(self, cutDt, limit):
        try:
            params = {
                "cutDt": cutDt,
                "endDt": self.endDt,
                "dataType": "inventory",
                "bodyFieldsOnly": False,
                "type": ["inventoryItem"],
                "limit": int(limit) if limit is not None else self.limit,
                "hours": self.hours
            }
            items = self._getRecords(params, funct=self.ns.getItems)
            rawProductsExtData = {}
            for item in items:
                data = self.transformData(item, self.map["inventory"])
                rawProductsExtData[data["sku"]] = {
                    "createdDate": data["createdDate"],
                    "lastModifiedDate": data["lastModifiedDate"],
                    "payload": data["locations"]
                }
            return rawProductsExtData
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def setPriceLevels(self, cutDt, limit):
        try:
            params = {
                "cutDt": cutDt,
                "endDt": self.endDt,
                "dataType": "pricelevels",
                "bodyFieldsOnly": False,
                "limit": int(limit) if limit is not None else self.limit,
                "hours": self.hours
            }
            items = self._getRecords(params, funct=self.ns.getItems)
            rawProductsExtData = {}
            for item in items:
                if item.pricingMatrix is not None:
                    priceLevels = [
                        {
                            "name": pricing.priceLevel.name,
                            "pricelist": [
                                {
                                    "price": str(price.value),
                                    "qty": price.quantity if price.quantity is not None else 1
                                } for price in pricing.priceList.price
                            ]
                        } for pricing in item.pricingMatrix.pricing
                    ]
                else:
                    priceLevels = []
                rawProductsExtData[item["itemId"]] = {
                    "createdDate": item["createdDate"].strftime('%Y-%m-%d %H:%M:%S'),
                    "lastModifiedDate": item["lastModifiedDate"].strftime('%Y-%m-%d %H:%M:%S'),
                    "payload": priceLevels
                }
            return rawProductsExtData
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        productsExtData = []
        rawProductsExtData = {}
        if dataType == "inventory":
            rawProductsExtData = self.setInventory(cutDt, (self.limit if limit is None else limit))
        elif dataType == "pricelevels":
            rawProductsExtData = self.setPriceLevels(cutDt, (self.limit if limit is None else limit))

        for sku, data in rawProductsExtData.items():
            try:
                productExtData = {}
                productExtData["sku"] = sku
                productExtData["frontend"] = frontend
                productExtData["data_type"] = dataType
                productExtData["create_dt"] = data["createdDate"]
                productExtData["update_dt"] = data["lastModifiedDate"]
                productExtData["data"] = data["payload"]
                productsExtData.append(productExtData)
            except Exception:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(
                    json.dumps(
                        {
                            "data_type": dataType,
                            "sku": sku,
                            "data": data
                        }
                    )
                )
                self.logger.error(productExtData)
                # raise
        return (productsExtData, rawProductsExtData)

    def boProductsExtDataExtFt(self, dataType, productsExtData, rawProductsExtData):
        pass

    def boCustomerFt(self, customer):
        return self.transformData(customer, self.map["customer"])

    def insertCustomersFt(self, newCustomers):
        boCustomers = self.ns.insertCustomers(newCustomers)
        for boCustomer in boCustomers:
            if boCustomer['tx_status'] == 'F':
                log = "Fail to insert an customer: %s/%s" % (boCustomer['fe_customer_id'], boCustomer['bo_customer_id'])
                self.logger.error(log)
                self.logger.error(boCustomer['tx_note'])
            else:
                log = "Successfully insert an customer: %s/%s" % (boCustomer['fe_customer_id'], boCustomer['bo_customer_id'])
                self.logger.info(log)
        return boCustomers

    def boOrderFt(self, order):
        # Transfer the data format.
        order = self.transformData(order, self.map["salesOrder"])

        # Map the payment method.
        order["paymentMethod"] = self.paymentMethods[
            order["paymentMethod"]
        ]

        # Map the shipping method.
        order["shipMethod"] = self.shipMethods[
            order["shipMethod"]
        ]

        # Map the payment term.
        order["terms"] = self._getTerm(order["paymentMethod"])

        # Map country code.
        order["billingAddress"]["country"] = self.countries[
            order["billingAddress"]["country"]
        ]
        order["shippingAddress"]["country"] = self.countries[
            order["shippingAddress"]["country"]
        ]
        return order

    def insertOrdersFt(self, newOrders):
        boOrders = self.ns.insertOrders(newOrders)
        for boOrder in boOrders:
            if boOrder['tx_status'] == 'F':
                log = "Fail to insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.error(log)
                self.logger.error(boOrder['tx_note'])
            else:
                log = "Successfully insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.info(log)
        return boOrders

    def boInvoicesFt(self, frontend, cutDt=None):
        try:
            params = {
                "cutDt": cutDt,
                "endDt": self.endDt,
                "limit": self.limit,
                "hours": self.hours
            }
            rawInvoices = self._getRecords(params, funct=self.ns.getInvoices)
            invoices = []
            for rawInvoice in rawInvoices:
                if rawInvoice["otherRefNum"] is not None:
                    invoice = {}
                    invoice["bo_invoice_id"] = rawInvoice["tranId"]
                    invoice["fe_order_id"] = rawInvoice["otherRefNum"]
                    invoice["frontend"] = frontend
                    invoice["data"] = self.transformData(rawInvoice, self.map["invoice"])
                    invoice["create_dt"] = invoice["data"]["createdDate"]
                    invoice["update_dt"] = invoice["data"]["lastModifiedDate"]
                    invoices.append(invoice)
            return (invoices, rawInvoices)
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def boInvoicesExtFt(self, invoices, rawInvoices):
        pass

    def boCustomersFt(self, frontend, cutDt=None):
        try:
            params = {
                "cutDt": cutDt,
                "endDt": self.endDt,
                "limit": self.limit,
                "hours": self.hours
            }
            rawCustomers = self._getRecords(params, funct=self.ns.getCustomers)
            customers = []
            for rawCustomer in rawCustomers:
                customer = {}
                customer["bo_customer_id"] = rawCustomer["entityId"]
                customer["fe_customer_id"] = rawCustomer["email"]
                customer["frontend"] = frontend
                customer["data"] = self.transformData(rawCustomer, self.map["customer"])
                customer["create_dt"] = customer["data"]["createdDate"]
                customer["update_dt"] = customer["data"]["lastModifiedDate"]
                customers.append(customer)
            return (customers, rawCustomers)
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def boCustomersExtFt(self, customers, rawCustomers):
        pass

    def boProductsFt(self, frontend, table, offset, limit, cutDt=None):
        """We could add validation into this function.
        """
        products = []
        headers = self.getMetadata(frontend, table)
        metadatas = dict(
            [(metadata["dest"], {"funct": metadata["funct"], "src": metadata["src"], "type": "attribute"}) for metadata in [header["metadata"] for header in headers]]
        )

        params = {
            "cutDt": cutDt,
            "endDt": self.endDt,
            "bodyFieldsOnly": False,
            "limit": int(limit) if limit is not None else self.limit,
            "hours": self.hours
        }
        if "DATATYPE" in self.setting.keys():
            params["dataType"] = self.setting["DATATYPE"]
        if "TYPE" in self.setting.keys():
            params["type"] = self.setting["TYPE"]
        if "VENDORNAME" in self.setting.keys():
            params["vendorName"] = self.setting["VENDORNAME"]
        if "ACTIVEONLY" in self.setting.keys():
            params["activeOnly"] = self.setting["ACTIVEONLY"]
        if "CUSTOMFIELDS" in self.setting.keys():
            params["customFields"] = self.setting["CUSTOMFIELDS"]

        rawData = self._getRecords(params, funct=self.ns.getItems)

        for rawProduct in rawData:
            try:
                data = self.transformData(rawProduct, metadatas)
                product = {}
                product["sku"] = data['sku']
                product["frontend"] = frontend
                product["table"] = table
                product["raw_data"] = {} # str(rawProduct.__dict__)
                product["data"] = data
                product["create_dt"] = rawProduct['createdDate'].strftime('%Y-%m-%d %H:%M:%S')
                product["update_dt"] = rawProduct['lastModifiedDate'].strftime('%Y-%m-%d %H:%M:%S')
                products.append(product)
            except Exception:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(data)
                self.logger.error(rawProduct)
                # raise
        return (products, rawData)

    def boProductsExtFt(self, products, rawProducts):
        pass

    # Sync Products from BackOffice to FrontEnd.
    def syncProductFt(self, product):
        data = product['data']
        itemType = data.pop("item_type", "inventoryItem")
        msrpPriceLevel = data.pop("msrp_price_level", None)
        data["customFields"] = {k: v for k, v in data.items() if k.find('cust') == 0}
        for k in data["customFields"].keys():
            data.pop(k)

        try:
            product['fe_product_id'] = self.ns.insertItem(data, itemType=itemType, msrpPriceLevel=msrpPriceLevel)
        except Exception:
            raise

    def feOrdersFt(self, cutDt):
        try:
            params = {
                "cutDt": cutDt,
                "endDt": self.endDt,
                "join": self.join,
                "limit": self.limit,
                "hours": self.hours
            }
            rawOrders = self._getRecords(params, funct=self.ns.getSalesOrders)
            orders = []
            for rawOrder in rawOrders:
                order = self.transformData(rawOrder, self.map["salesOrder"])
                orders.append(order)
            return (orders, rawOrders)
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return (orders, rawOrders)

    def feOrdersExtFt(self, orders, rawOrders):
        pass

