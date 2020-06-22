from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
import copy
import traceback

class DynamoDBAgency(FrontEnd, BackOffice):
    def __init__(self, setting={}, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        self.limit = int(setting.get("LIMIT", 100))
        if feConn is not None:
            self.dynamodb = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.dynamodb = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

    # Sync Products from BackOffice to FrontEnd.
    def syncProductFt(self, product):
        frontend = product['frontend']
        sku = product['sku']
        data = product['data']
        try:
            product['fe_product_id'] = self.dynamodb.syncProduct(frontend, sku, data)
        except Exception as e:
            raise

    def syncProductExtDataFt(self, productExtData):
        frontend = productExtData['frontend']
        sku = productExtData['sku']
        dataType = productExtData['data_type']
        data = productExtData['data']
        try:
            productExtData['fe_product_id'] = self.dynamodb.syncProductExtData(frontend, sku, dataType, data)
        except Exception as e:
            raise

    def boOrderFt(self, order):
        return order

    def boOrderExtFt(self, o, order):
        pass

    def boOrderLineFt(self, i, o, item, order):
        pass

    def boOrderLineExtFt(self, i, o, item, order):
        pass

    def boOrderIdFt(self, order):
        return self.dynamodb.getBoOrderId(order["fe_order_id"])

    def insertOrdersFt(self, newOrders):
        boOrders = self.dynamodb.insertOrders(newOrders)
        for boOrder in boOrders:
            if boOrder['tx_status'] == 'F':
                log = "Fail to insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.error(log)
                self.logger.error(boOrder['tx_note'])
            else:
                log = "Successfully insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.info(log)
        return boOrders

    # Sync PurchaseOrders from BackOffice to FrontEnd.
    def syncPurchaseOrderFt(self, purchaseOrder):
        purchaseOrder["fe_po_num"] = self.dynamodb.insertPurchaseOrder(purchaseOrder)

    # Sync Invoices from BackOffice to FrontEnd.
    def syncInvoiceFt(self, invoice):
        invoice["fe_invoice_id"] = self.dynamodb.insertInvoice(invoice)

    # Sync Customers from BackOffice to FrontEnd.
    def syncCustomerFt(self, customer):
        customer["fe_customer_id"] = self.dynamodb.insertCustomer(customer)

    def boProductsTotalFt(self, cutDt):
        totalCount = self.dynamodb.getTotalProductsCount(cutDt)
        return totalCount

    def boProductsFt(self, frontend, table, offset, limit, cutDt=None):
        """We could add validation into this function.
        """
        products = []
        headers = self.getMetadata(frontend, table)
        metadatas = dict(
            [(metadata["dest"], {"funct": metadata["funct"], "src": metadata["src"], "type": "attribute"}) for metadata in [header["metadata"] for header in headers]]
        )

        rawData = self.dynamodb.getProducts(
            cutDt,
            limit if limit is not None else self.limit,
            offset
        )
        for rawProduct in rawData:
            try:
                data = self.transformData(rawProduct["data"], metadatas)
                product = {}
                product["sku"] = rawProduct["sku"]
                product["frontend"] = frontend
                product["table"] = table
                product["raw_data"] = rawProduct["data"]
                product["data"] = data
                product["create_dt"] = rawProduct['create_dt']
                product["update_dt"] = rawProduct['update_dt']
                products.append(product)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(data)
                self.logger.error(rawProduct)
                # raise
        return (products, rawData)
