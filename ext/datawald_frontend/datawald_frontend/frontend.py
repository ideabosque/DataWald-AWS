from datawald_abstract import Abstract
from datetime import datetime
import traceback
import sys
from time import sleep
from decimal import Decimal
from cerberus import Validator, errors, SchemaError, TypeDefinition
Validator.types_mapping['decimal'] = TypeDefinition('decimal', (Decimal,), ())

class FrontEnd(Abstract):
    def __init__(self, logger=None, feApp=None, dataWald=None):
        """Return a new FrontEnd object.
        """
        self.feApp = feApp
        self.dataWald = dataWald
        self.logger = logger

    def setRawData(self, **params):
        pass

    def feOrdersFt(self, cutDt):
        return ([], [])

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    def getOrders(self, cutDt):
        """Gert orders from the fronted application.
        """
        (orders, rawOrders) = self.feOrdersFt(cutDt)
        self.feOrdersExtFt(orders, rawOrders)
        return orders

    def syncOrders(self, **params):
        backoffice = params.pop("backoffice")
        storeCode = params.pop("storeCode", "0")
        task = sys._getframe().f_code.co_name
        cutDt = self.dataWald.getLastCut(self.feApp, task)
        orders = self.getOrders(cutDt)
        entities = []
        for order in orders:
            try:
                feOrderId = order['fe_order_id']
                feOrderUpdateDate = order['fe_order_update_date']
                id = self.dataWald.syncOrder(self.feApp, feOrderId, order)
                log = "Successfully inserted document: %s/%s." % (id, feOrderId)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'fe_id': feOrderId,
                    'update_dt': feOrderUpdateDate
                }
                entities.append(entity)
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(order)

        if len(orders) > 0:
            lastOrder = max(orders, key=lambda order:datetime.strptime(order['fe_order_update_date'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastOrder['fe_order_update_date']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(backoffice, self.feApp, task, 'orders', syncTask)
        return {"_id": str(syncControlId), "table": "orders", "entities": entities}

    def reSyncOrders(self, id):
        return self.reSync(id,  self.dataWald.getOrder, "fe_id", primary="fe_order_id", updateDt="fe_order_update_date")

    # Sync customers from Frontend to Backoffice.
    def feCustomersFt(self, cutDt):
        return ([], [])

    def feCustomersExtFt(self, customers, rawCustomers):
        pass

    def getFECustomers(self, cutDt):
        """Gert customers from the fronted application.
        """
        (customers, rawCustomers) = self.feCustomersFt(cutDt)
        self.feCustomersExtFt(customers, rawCustomers)
        return customers

    def syncBOCustomers(self, **params):
        backoffice = params.pop("backoffice")
        storeCode = params.pop("storeCode", "0")
        task = sys._getframe().f_code.co_name
        cutDt = self.dataWald.getLastCut(self.feApp, task)
        customers = self.getFECustomers(cutDt)
        entities = []
        for customer in customers:
            try:
                feCustomerId = customer['fe_customer_id']
                feCustomerUpdateDate = customer['update_dt']
                id = self.dataWald.syncBOCustomer(self.feApp, feCustomerId, customer)
                log = "Successfully inserted document: %s/%s." % (id, feCustomerId)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'fe_id': feCustomerId,
                    'update_dt': feCustomerUpdateDate
                }
                entities.append(entity)
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(customer)

        if len(customers) > 0:
            lastCustomer = max(customers, key=lambda customer:datetime.strptime(customer['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastCustomer['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(backoffice, self.feApp, task, 'customers-bo', syncTask)
        return {"_id": str(syncControlId), "table": "customers", "entities": entities}

    def reSyncCustomers(self, id):
        return self.reSync(id, self.dataWald.getBOCustomer, "fe_customer_id")

    # Sync ItemReceipts from BackOffice to FrontEnd.
    def feItemReceiptsFt(self, cutDt):
        return ([], [])

    def feItemReceiptsExtFt(self, itemReceipts, rawItemReceipts):
        pass

    def getItemReceipts(self, cutDt=None):
        """Gert ItemReceipts from the fronted application.
        """
        (itemReceipts, rawItemReceipts) = self.feItemReceiptsFt(cutDt)
        self.feItemReceiptsExtFt(itemReceipts, rawItemReceipts)
        return itemReceipts

    def syncItemReceipts(self, **params):
        backoffice = params.pop("backoffice")
        storeCode = params.pop("storeCode", "0")
        task = sys._getframe().f_code.co_name
        cutDt = self.dataWald.getLastCut(self.feApp, task)
        itemReceipts = self.getItemReceipts(cutDt)
        entities = []
        for itemReceipt in itemReceipts:
            try:
                boPONum = itemReceipt['bo_po_num']
                updatedDt = itemReceipt['update_dt']
                id = self.dataWald.syncItemReceipt(self.feApp, boPONum, itemReceipt)  # Develop the function at aws_dwconnector.
                log = "Successfully inserted document: %s/%s." % (id, boPONum)
                self.logger.info(log)
                entity = {
                    'id': id,
                    'bo_po_num': boPONum,
                    'updated_dt': updatedDt
                }
                entities.append(entity)
            except Exception as e:
                self.logger.exception(e)
                self.logger.error(itemReceipt)

        if len(itemReceipts) > 0:
            lastItemReceipts = max(itemReceipts, key=lambda itemReceipt:datetime.strptime(itemReceipt['update_dt'], "%Y-%m-%d %H:%M:%S"))
            cutDt = lastItemReceipts['update_dt']
        syncTask = {
            "store_code": storeCode,
            "cut_dt": cutDt,
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(backoffice, self.feApp, task, 'itemreceipts', syncTask)
        return {"_id": str(syncControlId), "table": "itemreceipts", "entities": entities}

    def reSyncItemReceipts(self, id):
        return self.reSync(id, self.dataWald.getItemReceipt, "bo_po_num")

    # Sync customers from BackOffice to FrontEnd.
    def syncCustomerFt(self, customer):
        pass

    def syncFECustomers(self, **params):
        customers = params.pop("data")
        return self.syncFEEntities(
            "customer",
            customers,
            "fe_customer_id",
            "bo_customer_id",
            getEntity=self.dataWald.getFECustomer,
            syncFt=self.syncCustomerFt,
            updateEntityStatus=self.dataWald.updateFECustomerStatus
        )

    # Sync Invoices from BackOffice to FrontEnd.
    def syncInvoiceFt(self, invoice):
        pass

    def syncInvoices(self, **params):
        invoices = params.pop("data")
        return self.syncFEEntities(
            "invoice",
            invoices,
            "fe_invoice_id",
            "bo_invoice_id",
            getEntity=self.dataWald.getInvoice,
            syncFt=self.syncInvoiceFt,
            updateEntityStatus=self.dataWald.updateInvoiceStatus
        )

    # Sync Shipments from BackOffice to FrontEnd.
    def syncShipmentFt(self, shipment):
        pass

    def syncShipments(self, **params):
        shipments = params.pop("data")
        return self.syncFEEntities(
            "shipment",
            shipments,
            "fe_shipment_id",
            "bo_shipment_id",
            getEntity=self.dataWald.getShipment,
            syncFt=self.syncShipmentFt,
            updateEntityStatus=self.dataWald.updateShipmentStatus
        )

    # Sync PurchaseOrders from BackOffice to FrontEnd.
    def syncPurchaseOrderFt(self, purchaseOrder):
        pass

    def syncPurchaseOrders(self, **params):
        purchaseorders = params.pop("data")
        return self.syncFEEntities(
            "purchaseorder",
            purchaseorders,
            "fe_po_num",
            "bo_po_num",
            getEntity=self.dataWald.getPurchaseOrder,
            syncFt=self.syncPurchaseOrderFt,
            updateEntityStatus=self.dataWald.updatePurchaseOrderStatus
        )

    def validateProductData(self, product):
        headers = self.dataWald.getMetadata(self.feApp, product['table'])
        schema = {list(header["metadata"]["schema"].keys())[0]: list(header["metadata"]["schema"].values())[0] for header in headers if header["metadata"]["schema"] is not None}
        productValidate = Validator()
        # productValidate.allow_unknown = True
        if not productValidate.validate(product["data"], schema):
            raise Exception(productValidate.errors)
        else:
            pass

    # Sync Products from BackOffice to FrontEnd.
    def syncProductFt(self, product):
        pass

    def syncProducts(self, **params):
        products = params.pop("data")
        return self.syncFEEntities(
            "product",
            products,
            "fe_product_id",
            "sku",
            getEntity=self.dataWald.getProduct,
            syncFt=self.syncProductFt,
            updateEntityStatus=self.dataWald.updateProductStatus,
            validateData=self.validateProductData
        )

    @property
    def custOptValidation(self):
        return {
            "required": ['type','title'],
            "validation": {
                "type": ['field','area','file','drop_down','radio','checkbox','multiple','date','date_time','time'],
                "option_price_type": ['fixed','percent']
            }
        }

    @property
    def custOptValValidation(self):
        return {
            "required": [],
            "validation": {
                "option_value_price_type": ['fixed','percent']
            }
        }

    def validateData(self, sku, required, validation, data):
        if not set(required).issubset(set(data.keys())):
            error = {
                "sku": sku,
                "Customer Option Required": required,
                "Keys": data.keys(),
                "Error": "Miss required keys."
            }
            raise Exception(error)

        errors = []
        for key in data.keys():
            if key in validation.keys() and data[key] not in validation[key]:
                error = {
                    "sku": sku,
                    "Key": key,
                    "value": data[key],
                    "values": validation[key],
                    "Error": "The value({0}) is not supported.".format(data[key])
                }
                errors.append(error)

        if len(errors) > 0:
            raise Exception(errors)

    # Inspect custom option.
    def validateCustomOptions(self, productExtData):
        sku = productExtData["sku"]
        options = productExtData["data"]
        for option in options:
            self.validateData(sku, self.custOptValidation["required"], self.custOptValidation["validation"], option)

            if len(option["option_values"]) > 0:
                for optionValue in option["option_values"]:
                    self.validateData(sku, self.custOptValValidation["required"], self.custOptValValidation["validation"], optionValue)

    # Sync Products ExtData from BackOffice to FrontEnd.
    def syncProductExtDataFt(self, productExtData):
        pass

    def syncProductsExtData(self, **params):
        productsExtData = params.pop("data")
        dataType = productsExtData[0]["data_type"]
        return self.syncFEEntities(
            "product",
            productsExtData,
            "fe_product_id",
            "sku",
            getEntity=self.dataWald.getProductExtData,
            syncFt=self.syncProductExtDataFt,
            updateEntityStatus=self.dataWald.updateProductExtDataStatus,
            validateData=self.validateCustomOptions if dataType == "customoption" else None
        )
