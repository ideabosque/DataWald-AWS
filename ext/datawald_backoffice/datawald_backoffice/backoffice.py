from datawald_abstract import Abstract
from datetime import datetime
import traceback
import sys
from time import sleep


class BackOffice(Abstract):
    def __init__(self, logger=None, boApp=None, dataWald=None):
        self.boApp = boApp
        self.dataWald = dataWald
        self.logger = logger

    def setRawData(self, **params):
        pass

    def boOrderFt(self, order):
        return {}

    def boOrderExtFt(self, o, order):
        pass

    def boOrderLineFt(self, i, o, item, order):
        pass

    def boOrderLineExtFt(self, i, o, item, order):
        pass

    def boOrderIdFt(self, order):
        boOrderId = order["bo_order_id"] if "bo_order_id" in order.keys() \
            and order["bo_order_id"] != "####" else None
        return boOrderId

    def insertOrdersFt(self, newOrders):
        return None

    def txOrder(self, order):
        o = self.boOrderFt(order)
        self.boOrderExtFt(o, order)

        i = 0
        for item in order['items']:
            # GetEOrderLines
            self.boOrderLineFt(i, o, item, order)
            # GetEOrderLinesExt
            self.boOrderLineExtFt(i, o, item, order)
            i = i + 1
        return o

    def cancelOrderFt(self, boOrderId):
        pass

    def insertOrders(self, **params):
        orders = params.pop("data")
        return self.insertBOEntities(
            "order",
            orders,
            "fe_order_id",
            "bo_order_id",
            getEntity=self.dataWald.getOrder,
            getEntityId=self.boOrderIdFt,
            txEntity=self.txOrder,
            cancelEntity=self.cancelOrderFt,
            insertEntities=self.insertOrdersFt,
            updateEntityStatus=self.dataWald.updateOrderStatus
        )

    # Sync customers from Frontend to Backoffice.
    def boCustomerFt(self, customer):
        return {}

    def boCustomerExtFt(self, c, customer):
        pass

    def boCustomerIdFt(self, customer):
        boCustomerId = customer["bo_customer_id"] if "bo_customer_id" in c.keys() \
            and customer["bo_customer_id"] != "####" else None
        return boCustomerId

    def insertCustomersFt(self, newCustomers):
        return None

    def txCustomer(self, customer):
        c = self.boCustomerFt(customer)
        self.boCustomerExtFt(c, customer)
        return c

    def insertCustomers(self, **params):
        customers = params.pop("data")
        return self.insertBOEntities(
            "customer",
            customers,
            "fe_customer_id",
            "bo_customer_id",
            getEntity=self.dataWald.getBOCustomer,
            getEntityId=self.boCustomerIdFt,
            txEntity=self.txCustomer,
            insertEntities=self.insertCustomersFt,
            updateEntityStatus=self.dataWald.updateBOCustomerStatus
        )

    def boItemReceiptFt(self, itemReceipt):
        return {}

    def boItemReceiptExtFt(self, ir, itemReceipt):
        pass

    def boItemReceiptIdFt(self, itemReceipt):
        boItemreceiptId = []
        if "bo_itemreceipt_id" in itemReceipt.keys():
            boItemreceiptId = itemReceipt["bo_itemreceipt_id"]
        return boItemreceiptId

    def insertItemReceiptsFt(self, newItemReceipts):
        return None

    def txItemReceipt(self, itemReceipt):
        ir = self.boItemReceiptFt(itemReceipt)
        self.boItemReceiptExtFt(ir, itemReceipt)
        return ir

    def insertItemReceipts(self, **params):
        itemReceipts = params.pop("data")
        return self.insertBOEntities(
            "itemreceipt",
            itemReceipts,
            "bo_po_num",
            "bo_itemreceipt_id",
            stack=True,
            getEntity=self.dataWald.getItemReceipt,
            getEntityId=self.boItemReceiptIdFt,
            txEntity=self.txItemReceipt,
            insertEntities=self.insertItemReceiptsFt,
            updateEntityStatus=self.dataWald.updateItemReceiptStatus
        )

    def getMetadata(self, frontend, table):
        return self.dataWald.getMetadata(frontend, table)

    def boProductsTotalFt(self, cutDt):
        return 0

    def boProductsFt(self, frontend, table, offset, limit, cutDt=None):
        return ([], [])

    def boProductsExtFt(self, products, rawProducts):
        pass

    def getProducts(self, frontend, table, offset, limit, cutDt=None):
        """Get products from the back office application.
        """
        (products, rawProducts) = self.boProductsFt(frontend, table, offset, limit, cutDt)  # Need update function.
        self.boProductsExtFt(products, rawProducts)
        return products

    def retrieveProducts(self, **params):
        frontend = params.pop("frontend")
        table = params.pop("table")
        storeCode = params.pop("storeCode", "0")
        limit = params.pop("limit", None)
        #task = sys._getframe().f_code.co_name
        task = 'syncProducts'
        (offset, cutDt) = self.dataWald.getLastCut(frontend, task, offset=True)  # Need update function.
        totalRecord = self.boProductsTotalFt(cutDt)  # Need update function.
        self.logger.info("Total:{0} Offset:{1} Limit:{2} Cut Date:{3}".format(totalRecord,offset,limit,cutDt))
        products = self.getProducts(frontend, table, offset, limit, cutDt)
        entities = []
        for product in products:
            try:
                sku = product['sku']
                updateDt = product['update_dt']
                id = self.dataWald.syncProduct(self.boApp, frontend, sku, product)
                log = "Successfully inserted document: %s/%s." % (id, sku)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'sku': sku,
                    'update_dt': updateDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(product)
        if len(products) > 0:
            offset = offset + len(products)
            if offset >= totalRecord:
                lastProduct = max(products, key=lambda product:datetime.strptime(product['update_dt'], "%Y-%m-%d %H:%M:%S"))
                cutDt = lastProduct['update_dt']
                offset = 0
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "offset": offset,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, 'products', syncTask)
        return {"id": str(syncControlId), "table": table, "entities": entities, "offset": offset}

    def reSyncProducts(self, id):
        return self.reSync(id, self.dataWald.getProduct, 'sku')

    def boProductsExtDataTotalFt(self, dataType, cutDt):
        return 0

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        return ([], [])

    def boProductsExtDataExtFt(self, dataType, productsExtData, rawProductsExtData):
        pass

    def getProductsExtData(self, frontend, dataType, offset, limit, cutDt=None):
        """Get product's data from the back office application.
        """
        (productsExtData, rawProductsExtData) = self.boProductsExtDataFt(frontend, dataType, offset, limit, cutDt)
        self.boProductsExtDataExtFt(dataType, productsExtData, rawProductsExtData)
        return productsExtData

    def retrieveProductsExtData(self, **params):
        frontend = params.pop("frontend")
        dataType = params.pop("dataType")
        storeCode = params.pop("storeCode", "0")
        limit = params.pop("limit", None)
        #task = sys._getframe().f_code.co_name
        task = 'syncProductExtData-{}'.format(dataType)
        (offset, cutDt) = self.dataWald.getLastCut(frontend, task, offset=True)
        totalRecord = self.boProductsExtDataTotalFt(dataType, cutDt)
        self.logger.info("Total:{0} Offset:{1} Limit:{2} Cut Date:{3}".format(totalRecord,offset,limit,cutDt))
        productsExtData = self.getProductsExtData(frontend, dataType, offset, limit, cutDt)
        entities = []
        for productExtData in productsExtData:
            try:
                sku = productExtData['sku']
                updateDt = productExtData['update_dt']
                id = self.dataWald.syncProductExtData(self.boApp, frontend, sku, dataType, productExtData) # Develop the function.
                log = "Successfully inserted document: %s/%s." % (id, sku)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'sku': sku,
                    'update_dt': updateDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(productExtData)
        if len(productsExtData) > 0:
            offset = offset + len(productsExtData)
            if offset >= totalRecord:
                lastProductExtData = max(productsExtData, key=lambda productExtData:datetime.strptime(productExtData['update_dt'], "%Y-%m-%d %H:%M:%S"))
                cutDt = lastProductExtData['update_dt']
                offset = 0
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "offset": offset,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, "{0}-{1}".format("products", dataType), syncTask)
        return {"id": str(syncControlId), "data_type": dataType, "entities": entities, "offset": offset}

    def boInvoicesFt(self, frontend, cutDt=None):
        return ([], [])

    def boInvoicesExtFt(self, invoices, rawInvoices):
        pass

    def getInvoices(self, frontend, cutDt=None):
        """Get invoices from the back office application.
        """
        (invoices, rawInvoices) = self.boInvoicesFt(frontend, cutDt)
        self.boInvoicesExtFt(invoices, rawInvoices)
        return invoices

    def retrieveInvoices(self, **params):
        frontend = params.pop("frontend")
        storeCode = params.pop("storeCode", "0")
        task = 'syncInvoices'
        cutDt = self.dataWald.getLastCut(frontend, task)
        invoices = self.getInvoices(frontend, cutDt=cutDt)
        entities = []
        for invoice in invoices:
            try:
                boInvoiceId = invoice['bo_invoice_id']
                createDt = invoice['update_dt']
                id = self.dataWald.syncInvoice(self.boApp, frontend, boInvoiceId, invoice)    # Develop the function.
                log = "Successfully inserted document: %s/%s." % (id, boInvoiceId)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'bo_invoice_id': boInvoiceId,
                    'update_dt': createDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(invoice)

        if len(invoices) > 0:
            lastInvoice = max(invoices, key=lambda invoice:datetime.strptime(invoice['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastInvoice['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, 'invoices', syncTask)
        return {"id": str(syncControlId), "frontend": frontend, "entities": entities}

    def reSyncInvoices(self, id):
        return self.reSync(id, self.dataWald.getInvoice, 'bo_invoice_id')

    def boShipmentsFt(self, frontend, cutDt=None):
        return ([], [])

    def boShipmentsExtFt(self, shipments, rawShipments):
        pass

    def getShipments(self, frontend, cutDt=None):
        """Get shipments from the back office application.
        """
        (shipments, rawShipments) = self.boShipmentsFt(frontend, cutDt)
        self.boShipmentsExtFt(shipments, rawShipments)
        return shipments

    def retrieveShipments(self, **params):
        frontend = params.pop("frontend")
        storeCode = params.pop("storeCode", "0")
        task = 'syncShipments'
        cutDt = self.dataWald.getLastCut(frontend, task)
        shipments = self.getShipments(frontend, cutDt=cutDt)
        entities = []
        for shipment in shipments:
            try:
                boShipmentId = shipment['bo_shipment_id']
                createDt = shipment['update_dt']
                id = self.dataWald.syncShipment(self.boApp, frontend, boShipmentId, shipment)    # Develop the function.
                log = "Successfully inserted document: %s/%s." % (id, boShipmentId)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'bo_shipment_id': boShipmentId,
                    'update_dt': createDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(shipment)

        if len(shipments) > 0:
            lastShipment = max(shipments, key=lambda shipment:datetime.strptime(shipment['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastShipment['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, 'shipments', syncTask)
        return {"id": str(syncControlId), "frontend": frontend, "entities": entities}

    def reSyncShipments(self, id):
        return self.reSync(id, self.dataWald.getShipment, 'bo_shipment_id')

    def boPurchaseOrdersFt(self, frontend, cutDt=None):
        return ([], [])

    def boPurchaseOrdersExtFt(self, purchaseOrders, rawPurchaseOrders):
        pass

    def getPurchaseOrders(self, frontend, cutDt=None):
        """Get purchaseorders from the back office application.
        """
        (purchaseOrders, rawPurchaseOrders) = self.boPurchaseOrdersFt(frontend, cutDt)
        self.boPurchaseOrdersExtFt(purchaseOrders, rawPurchaseOrders)
        return purchaseOrders

    def retrievePurchaseOrders(self, **params):
        frontend = params.pop("frontend")
        storeCode = params.pop("storeCode", "0")
        task = 'syncPurchaseOrders'
        cutDt = self.dataWald.getLastCut(frontend, task)
        purchaseOrders = self.getPurchaseOrders(frontend, cutDt=cutDt)
        entities = []
        for purchaseOrder in purchaseOrders:
            try:
                boPONum = purchaseOrder['bo_po_num']
                createDt = purchaseOrder['update_dt']
                id = self.dataWald.syncPurchaseOrder(self.boApp, frontend, boPONum, purchaseOrder)    # Develop the function.
                log = "Successfully inserted document: %s/%s." % (id, boPONum)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'bo_po_num': boPONum,
                    'update_dt': createDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(purchaseOrder)
        if len(purchaseOrders) > 0:
            lastPurchaseOrder = max(purchaseOrders, key=lambda purchaseOrder:datetime.strptime(purchaseOrder['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastPurchaseOrder['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, 'purchaseorders', syncTask)
        return {"id": str(syncControlId), "frontend": frontend, "entities": entities}

    def reSyncPurchaseOrders(self, id):
        pass

    # Sync customers from BackOffice to Frontend.
    def boCustomersFt(self, frontend, cutDt=None):
        return ([], [])

    def boCustomersExtFt(self, customers, rawCustomers):
        pass

    def getBOCustomers(self, frontend, cutDt=None):
        """Get customers from the back office application.
        """
        (customers, rawCustomers) = self.boCustomersFt(frontend, cutDt)
        self.boCustomersExtFt(customers, rawCustomers)
        return customers

    def retrieveCustomers(self, **params):
        frontend = params.pop("frontend")
        storeCode = params.pop("storeCode", "0")
        task = 'syncFECustomers'
        cutDt = self.dataWald.getLastCut(frontend, task)
        customers = self.getBOCustomers(frontend, cutDt=cutDt)
        entities = []
        for customer in customers:
            try:
                boCustomerId = customer['bo_customer_id']
                createDt = customer['update_dt']
                id = self.dataWald.syncFECustomer(self.boApp, frontend, boCustomerId, customer)    # Develop the function.
                log = "Successfully inserted document: %s/%s." % (id, boCustomerId)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'bo_customer_id': boCustomerId,
                    'update_dt': createDt
                }
                entities.append(entity)
            except Exception as e:
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(customer)

        if len(customers) > 0:
            lastCustomer = max(customers, key=lambda customer:datetime.strptime(customer['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastCustomer['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(self.boApp, frontend, task, 'customers-fe', syncTask)
        return {"id": str(syncControlId), "frontend": frontend, "entities": entities}

    def reSyncCustomers(self, id):
        return self.reSync(id, self.dataWald.getFECustomer, 'bo_customer_id')
