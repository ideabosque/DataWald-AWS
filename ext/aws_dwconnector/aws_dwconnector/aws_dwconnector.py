import requests, base64, json, time, traceback, boto3, hashlib, hmac
from datetime import datetime, date
from decimal import Decimal

# Helper class to convert a DynamoDB item to JSON.
class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        elif isinstance(o, (datetime, date)):
            return o.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(o, (bytes, bytearray)):
            return str(o)
        else:
            return super(JSONEncoder, self).default(o)


class JSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        json.JSONDecoder.__init__(self, object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, o):
        if '_type' not in o:
            return o
        type = o['_type']
        if type in ['bytes', 'bytearray']:
            return str(o['value'])
        return o


class DWConnector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger
        self.expiresTimeTS = time.time()
        self._idToken = None

    def connect(self):
        if "DWUSERPOOLID" in self.setting.keys():
            if self._idToken is None or (self.expiresTimeTS - time.time()) <= 0:
                data = self.getTokenId()
                self.expiresTimeTS = time.time() + data["expiresIn"]
                self._idToken = data["idToken"]
            return {
                "Authorization": self._idToken,
                "Content-Type": "application/json",
                "x-api-key": self.setting['DWAPIKEY']
            }
        else:
            return {
                'Accept': 'application/json',
                "Content-Type": "application/json",
                "x-api-key": self.setting['DWAPIKEY']
            }


    def getTokenId(self):
        digest = hmac.new(
            self.setting['DWSECRETKEY'],
            msg=self.setting['DWRESTUSR'] + self.setting['DWCLIENTID'],
            digestmod=hashlib.sha256
        ).digest()
        signature = base64.b64encode(digest).decode()
        try:
            response = boto3.client('cognito-idp').admin_initiate_auth(
                UserPoolId=self.setting['DWUSERPOOLID'],
                ClientId=self.setting['DWCLIENTID'],
                AuthFlow='ADMIN_NO_SRP_AUTH',
                AuthParameters={
                    'USERNAME': self.setting['DWRESTUSR'],
                    'PASSWORD': self.setting['DWRESTPASS'],
                    'SECRET_HASH': signature
                },
            )
            expiresIn = response["AuthenticationResult"]["ExpiresIn"]
            idToken = response["AuthenticationResult"]["IdToken"]
            return {"expiresIn": expiresIn, "idToken": idToken}
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise Exception(log)

    def _jsonDumps(self, data):
        return json.dumps(data, indent=4, cls=JSONEncoder, ensure_ascii=False)

    def _jsonLoads(self, data):
        return json.loads(data, cls=JSONDecoder)

    def insertConfigEntity(self, configEntity):
        requestUrl = "{}/core/config".format(self.setting['DWRESTENDPOINT'])
        response = requests.post(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(configEntity),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 200:
            configEntity = self._jsonLoads(response.content)
            return configEntity
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getConfigEntity(self, key):
        queryString = {"key": key}
        requestUrl = "{}/core/config".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            configEntity = self._jsonLoads(response.content)
            return configEntity
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateConfigEntity(self, key, configEntity):
        queryString = {"key": key}
        requestUrl = "{}/core/config".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(configEntity),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            configEntity = self._jsonLoads(response.content)
            return configEntity
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def delConfigEntity(self, key):
        queryString = {"key": key}
        requestUrl = "{}/core/config".format(self.setting['DWRESTENDPOINT'])
        response = requests.delete(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            log = response.content
            return log
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def insertConnection(self, connection):
        requestUrl = "{}/core/connections".format(self.setting['DWRESTENDPOINT'])
        response = requests.post(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(connection),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 200:
            connection = self._jsonLoads(response.content)
            return connection
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getConnection(self, area, id):
        queryString = {
            "area": area,
            "id": id
        }
        requestUrl = "{}/core/connections".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            metadata = self._jsonLoads(response.content)
            return metadata
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateConnection(self, area, id, connection):
        queryString = {
            "area": area,
            "id": id
        }
        requestUrl = "{}/core/connections".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(connection),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            connection = self._jsonLoads(response.content)
            return connection
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def delConnection(self, area, id):
        queryString = {
            "area": area,
            "id": id
        }
        requestUrl = "{}/core/connections".format(self.setting['DWRESTENDPOINT'])
        response = requests.delete(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            log = response.content
            return log
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def insertMetadataEntity(self, metadataEntty):
        requestUrl = "{}/core/productmastermetadata".format(self.setting['DWRESTENDPOINT'])
        response = requests.post(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(metadataEntty),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 200:
            metadataEntty = self._jsonLoads(response.content)
            return metadataEntty
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getMetadata(self, frontend, table):
        queryString = {
            "frontend": frontend,
            "table": table
        }
        requestUrl = "{}/core/productmastermetadata".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            metadata = self._jsonLoads(response.content)
            return metadata
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateMetadataEntity(self, frontend, column, metadataEntty):
        queryString = {
            "frontend": frontend,
            "column": column
        }
        requestUrl = "{}/core/productmastermetadata".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(metadataEntty),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            metadataEntty = self._jsonLoads(response.content)
            return metadataEntty
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def delMetadataEntity(self, frontend, column):
        queryString = {
            "frontend": frontend,
            "column": column
        }
        requestUrl = "{}/core/productmastermetadata".format(self.setting['DWRESTENDPOINT'])
        response = requests.delete(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            log = response.content
            return log
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getTask(self, table, id):
        queryString = {
            "table": table,
            "id": id
        }
        requestUrl = "{}/control/task".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            task = self._jsonLoads(response.content)
            return task
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getLastCut(self, frontend, task, offset=False):
        queryString = {
            "frontend": frontend,
            "task": task
        }
        requestUrl = "{}/control/cutdt".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            if offset:
                return(int(data['offset']), data['cut_dt'])
            else:
                return data['cut_dt']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def insertSyncControl(self, backoffice, frontend, task, table, syncTask):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "task": task,
            "table": table
        }
        requestUrl = "{}/control/synccontrol".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(syncTask),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            id = response.content
            return id
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateSyncTask(self, id, entities):
        queryString = {
            "id": id
        }
        requestUrl = "{}/control/synctask".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(entities),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getSyncTask(self, id):
        queryString = {
            "id": id
        }
        requestUrl = "{}/control/synctask".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            syncTask = self._jsonLoads(response.content)
            return syncTask
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def delSyncTask(self, id):
        queryString = {
            "id": id
        }
        requestUrl = "{}/control/synctask".format(self.setting['DWRESTENDPOINT'])
        response = requests.delete(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            log = response.content
            return log
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncOrder(self, frontend, feOrderId, order):
        queryString = {
            "frontend": frontend,
            "feorderid": feOrderId
        }
        requestUrl = "{}/backoffice/order".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(order).encode('utf-8'),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            order = self._jsonLoads(response.content)
            return order['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getOrder(self, frontend, feOrderId):
        queryString = {
            "frontend": frontend,
            "feorderid": feOrderId
        }
        requestUrl = "{}/backoffice/order".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            order = self._jsonLoads(response.content)
            return order
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateOrderStatus(self, id, orderStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/backoffice/orderstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(orderStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncBOCustomer(self, frontend, feCustomerId, customer):
        queryString = {
            "frontend": frontend,
            "fecustomerid": feCustomerId
        }
        requestUrl = "{}/backoffice/customer".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(customer),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            customer = self._jsonLoads(response.content)
            return customer['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getBOCustomer(self, frontend, feCustomerId):
        queryString = {
            "frontend": frontend,
            "fecustomerid": feCustomerId
        }
        requestUrl = "{}/backoffice/customer".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            customer = self._jsonLoads(response.content)
            return customer
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateBOCustomerStatus(self, id, customerStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/backoffice/customerstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(customerStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncFECustomer(self, backoffice, frontend, boCustomerId, customer):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "bocustomerid": boCustomerId
        }
        requestUrl = "{}/frontend/customer".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(customer),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            customer = self._jsonLoads(response.content)
            return customer['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getFECustomer(self, frontend, boCustomerId):
        queryString = {
            "frontend": frontend,
            "bocustomerid": boCustomerId
        }
        requestUrl = "{}/frontend/customer".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            customer = self._jsonLoads(response.content)
            return customer
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateFECustomerStatus(self, id, customerStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/frontend/customerstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(customerStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncShipment(self, backoffice, frontend, boShipmentId, shipment):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "boshipmentid": boShipmentId
        }
        requestUrl = "{}/frontend/shipment".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(shipment),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            shipment = self._jsonLoads(response.content)
            return shipment['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getShipment(self, frontend, boShipmentId):
        queryString = {
            "frontend": frontend,
            "boshipmentid": boShipmentId
        }
        requestUrl = "{}/frontend/shipment".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            shipment = self._jsonLoads(response.content)
            return shipment
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateShipmentStatus(self, id, shipmentStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/frontend/shipmentstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(shipmentStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncInvoice(self, backoffice, frontend, boInvoiceId, invoice):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "boinvoiceid": boInvoiceId
        }
        requestUrl = "{}/frontend/invoice".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(invoice),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            invoice = self._jsonLoads(response.content)
            return invoice['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getInvoice(self, frontend, boInvoiceId):
        queryString = {
            "frontend": frontend,
            "boinvoiceid": boInvoiceId
        }
        requestUrl = "{}/frontend/invoice".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            invoice = self._jsonLoads(response.content)
            return invoice
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateInvoiceStatus(self, id, invoiceStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/frontend/invoicestatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(invoiceStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncPurchaseOrder(self, backoffice, frontend, boPONum, purchaseOrder):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "boponum": boPONum
        }
        requestUrl = "{}/frontend/purchaseorder".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(purchaseOrder),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            purchaseOrder = self._jsonLoads(response.content)
            return purchaseOrder['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getPurchaseOrder(self, frontend, boPONum):
        queryString = {
            "frontend": frontend,
            "boponum": boPONum
        }
        requestUrl = "{}/frontend/purchaseorder".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            purchaseOrder = self._jsonLoads(response.content)
            return purchaseOrder
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updatePurchaseOrderStatus(self, id, purchaseOrderStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/frontend/purchaseorderstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(purchaseOrderStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncItemReceipt(self, frontend, boPONum, itemReceipt):
        queryString = {
            "frontend": frontend,
            "boponum": boPONum
        }
        requestUrl = "{}/backoffice/itemreceipt".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(itemReceipt),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            itemReceipt = self._jsonLoads(response.content)
            return itemReceipt['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getItemReceipt(self, frontend, boPONum):
        queryString = {
            "frontend": frontend,
            "boponum": boPONum
        }
        requestUrl = "{}/backoffice/itemreceipt".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            itemReceipt = self._jsonLoads(response.content)
            return itemReceipt
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateItemReceiptStatus(self, id, itemReceiptStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/backoffice/itemreceiptstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(itemReceiptStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncProduct(self, backoffice, frontend, sku, product):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "sku": sku
        }
        requestUrl = "{}/frontend/product".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    json=self._jsonDumps(product),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            product = self._jsonLoads(response.content)
            return product['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getProduct(self, frontend, sku):
        queryString = {
            "frontend": frontend,
            "sku": sku
        }
        requestUrl = "{}/frontend/product".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            product = self._jsonLoads(response.content)
            return product
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateProductStatus(self, id, productStatus):
        queryString = {
            "id": id
        }
        requestUrl = "{}/frontend/productstatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(productStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def syncProductExtData(self, backoffice, frontend, sku, dataType, productExtData):
        queryString = {
            "backoffice": backoffice,
            "frontend": frontend,
            "sku": sku,
            "datatype": dataType
        }
        requestUrl = "{}/frontend/productextdata".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    #json = productExtData,
                                    data=self._jsonDumps(productExtData),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            productExtData = self._jsonLoads(response.content)
            return productExtData['id']
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getProductExtData(self, frontend, sku, dataType):
        queryString = {
            "frontend": frontend,
            "sku": sku,
            "datatype": dataType
        }
        requestUrl = "{}/frontend/productextdata".format(self.setting['DWRESTENDPOINT'])
        response = requests.get(requestUrl, headers=self.headers, params=queryString)
        if response.status_code == 200:
            productExtData = self._jsonLoads(response.content)
            return productExtData
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def updateProductExtDataStatus(self, id, productExtDataStatus):
        dataType = productExtDataStatus.pop('data_type',None)
        queryString = {
            "id": id,
            "datatype": dataType
        }
        requestUrl = "{}/frontend/productextdatastatus".format(self.setting['DWRESTENDPOINT'])
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=self._jsonDumps(productExtDataStatus),
                                    timeout=60,
                                    verify=True,
                                    params=queryString
                                )
        if response.status_code == 200:
            data = self._jsonLoads(response.content)
            return data
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    @property
    def headers(self):
        return self.connect()
