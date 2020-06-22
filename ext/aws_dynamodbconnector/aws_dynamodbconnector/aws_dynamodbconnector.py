import boto3, json, uuid, copy, traceback
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal

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

class DynamoDBConnector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger
        self.metadata = {
            "products": {
                "partition": "source",
                "key": "sku",
                "index": "sku_index"
            },
            "products-inventory": {
                "partition": "source",
                "key": "sku",
                "index": "sku_index"
            },
            "products-imagegallery": {
                "partition": "source",
                "key": "sku",
                "index": "sku_index"
            },
            "products-pricelevels": {
                "partition": "source",
                "key": "sku",
                "index": "sku_index"
            },
            "purchaseorders": {
                "partition": "source",
                "key": "bo_po_num",
                "index": "bo_po_num_index"
            },
            "orders": {
                "partition": "source",
                "key": "fe_order_id",
                "index": "fe_order_id_index"
            },
            "invoices": {
                "partition": "source",
                "key": "fe_order_id",
                "index": "fe_order_id_index"
            },
            "customers": {
                "partition": "source",
                "key": "fe_customer_id",
                "index": "fe_customer_id_index"
            }
        }

    def connect(self):
        regionName = self.setting['REGIONNAME'] if self.setting is not None and 'REGIONNAME' in self.setting.keys() else None
        awsAccessKeyId = self.setting['AWSACCESSKEYID'] if self.setting is not None and 'AWSACCESSKEYID' in self.setting.keys() else None
        awsSecretAccessKey = self.setting['AWSSECRETACCESSKEY'] if self.setting is not None and 'AWSSECRETACCESSKEY' in self.setting.keys() else None

        if regionName is not None and awsAccessKeyId is not None and awsSecretAccessKey is not None:
            return boto3.resource(
                'dynamodb',
                region_name=regionName,
                aws_access_key_id=awsAccessKeyId,
                aws_secret_access_key=awsSecretAccessKey
            )
        else:
            return boto3.resource('dynamodb')

    @property
    def dynamodb(self):
        return self.connect()

    def getCount(self, tableType, source=None, updateDt=None):
        tableName = self.setting["tables"][tableType]
        partition = self.metadata[tableType]["partition"]
        index = "update_dt_index"
        table = self.dynamodb.Table(tableName)
        response = table.query(
            IndexName=index,
            KeyConditionExpression=Key(partition).eq(source) & Key('update_dt').gte(updateDt)
        )
        return response['Count']

    def deleteItems(self, tableType, items):
        tableName = self.setting["tables"][tableType]
        table = self.dynamodb.Table(tableName)
        for item in items:
            table.delete_item(
                Key={'id': item["id"]}
            )
            self.logger.info("Delete item({})".format(item["id"]))

    def getItem(self, tableType, value, source=None):
        tableName = self.setting["tables"][tableType]
        key = self.metadata[tableType]["key"]
        index = self.metadata[tableType]["index"]
        table = self.dynamodb.Table(tableName)
        item = None
        if source is not None:
            response = table.query(
                IndexName=index,
                KeyConditionExpression=Key(key).eq(value),
                FilterExpression=Attr('source').eq(source)
            )
        else:
            response = table.query(
                IndexName=index,
                KeyConditionExpression=Key(key).eq(value)
            )

        if response['Count'] == 1:
            item = response["Items"][0]
        elif response['Count'] > 1 and source is not None:
            lastItem = max(
                response["Items"],
                key=lambda item: item['update_dt']
            )
            for duplicatedItem in list(filter(lambda x: (x["id"] != lastItem["id"]) ,response["Items"])):
                table.delete_item(Key={'id': duplicatedItem["id"]})
            item = lastItem
        elif response['Count'] > 1 and source is None:
            raise Exception("There are more then one record for the key({}).  Please add source into the query".format(value))
        return item

    def getItems(self, tableType, limit, offset, source=None, updateDt=None):
        tableName = self.setting["tables"][tableType]
        partition = self.metadata[tableType]["partition"]
        index = "update_dt_index"
        table = self.dynamodb.Table(tableName)
        response = table.query(
            IndexName=index,
            KeyConditionExpression=Key(partition).eq(source) & Key('update_dt').gte(updateDt)
        )
        if response['Count'] != 0:
            items = sorted(response["Items"], key=lambda x: x['update_dt'], reverse=False)
            offset = int(offset)
            limit = int(limit)
            return items[offset:offset+limit]
        else:
            return []

    def putItem(self, tableType, source=None, value=None, data=None):
        tableName = self.setting["tables"][tableType]
        partition = self.metadata[tableType]["partition"]
        key = self.metadata[tableType]["key"]
        index = self.metadata[tableType]["index"]
        table = self.dynamodb.Table(tableName)
        createDt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        updateDt = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        _id = uuid.uuid1().int>>64

        response = table.query(
            IndexName=index,
            KeyConditionExpression=Key(key).eq(value),
            FilterExpression=Attr('source').eq(source)
        )

        # Check if the entity is exist or not.
        if response['Count'] == 1:
            item = response["Items"][0]
            createDt = item["create_dt"]
            _id = item["id"]
        elif response['Count'] > 1 and source is not None:
            item = max(
                response["Items"],
                key=lambda item: item['update_dt']
            )
            createDt = item["create_dt"]
            _id = item["id"]
            for duplicatedItem in list(filter(lambda x: (x["id"] != item["id"]) ,response["Items"])):
                table.delete_item(Key={'id': duplicatedItem["id"]})

        entity = {
            "id": _id,
            partition: source,
            key: value,
            "data": json.loads(json.dumps(data, cls=JSONEncoder), parse_float=Decimal),
            "create_dt": createDt,
            "update_dt": updateDt
        }
        table.put_item(Item=entity)

        # If updating products, the updateDt of the associated data will be updated.
        # if response["Count"] == 0 and tableType == "products":
        if tableType == "products":
            productExtData = {
                "imagegallery": self._getProductExtData("products-imagegallery", value) \
                    if self.setting["tables"].get("products-imagegallery", None) is not None else None,
                "inventory": self._getProductExtData("products-inventory", value) \
                    if self.setting["tables"].get("products-inventory", None) is not None else None,
                "pricelevels": self._getProductExtData("products-pricelevels", value) \
                    if self.setting["tables"].get("products-pricelevels", None) is not None else None
            }
            for k, v in productExtData.items():
                if v is not None:
                    self.logger.info("{0}/{1}: {2}".format(k, value, v["data"]))
                    self.syncProductExtData(v["source"], value, k, v["data"])

        return _id

    def _getProductExtData(self, dataType, sku):
        try:
            return self.getItem(dataType, sku)
        except:
            log = traceback.format_exc()
            self.logger.exception(log)
            return None

    def syncProduct(self, frontend, sku, data):
        return self.putItem("products", source=frontend, value=sku, data=data)

    def syncProductExtData(self, frontend, sku, dataType, data):
        if dataType == "inventory":
            return self.putItem("products-inventory", source=frontend, value=sku, data=data)
        elif dataType == "imagegallery":
            return self.putItem("products-imagegallery", source=frontend, value=sku, data=data)
        elif dataType == "pricelevels":
            return self.putItem("products-pricelevels", source=frontend, value=sku, data=data)

    def getBoOrderId(self, feOrderId):
        item = self.getItem("orders", feOrderId)
        if item is not None:
            return item["id"]
        else:
            return None

    def insertOrder(self, frontend, feOrderId, data):
        data.pop("tx_dt")
        data.pop("tx_status")
        data.pop("tx_note")
        return self.putItem("orders", source=frontend, value=feOrderId, data=data)

    def insertOrders(self, orders):
        for order in orders:
            data = copy.deepcopy(order)
            order["bo_order_id"] = self.insertOrder(order["frontend"], order["fe_order_id"], data)
            order["tx_status"] = 'S'
        return orders

    def insertPurchaseOrder(self, purchaseOrder):
        purchaseOrder.pop("tx_dt")
        purchaseOrder.pop("tx_status")
        purchaseOrder.pop("tx_note")
        source = purchaseOrder.get("frontend")
        boPONum = purchaseOrder.get("bo_po_num")
        data = purchaseOrder.get("data")
        purchaseOrderNumber = self.putItem(
            "purchaseorders",
            source=source,
            value=boPONum,
            data=data
        )
        self.logger.info("Invoice created successfully, purchase order number: {purchaseorder_number}".format(
                purchaseorder_number=purchaseOrderNumber
            )
        )
        return purchaseOrderNumber

    def insertInvoice(self, invoice):
        invoice.pop("tx_dt")
        invoice.pop("tx_status")
        invoice.pop("tx_note")
        source = invoice.get("frontend")
        feOrderId = invoice.get("fe_order_id")
        data = invoice.get("data")
        data["bo_invoice_id"] = invoice.get("bo_invoice_id")
        invoiceNumber = self.putItem(
            "invoices",
            source=source,
            value=feOrderId,
            data=data
        )
        self.logger.info("Invoice created successfully, invoice number: {invoice_number}".format(
                invoice_number=invoiceNumber
            )
        )
        return invoiceNumber

    def insertCustomer(self, customer):
        customer.pop("tx_dt")
        customer.pop("tx_status")
        customer.pop("tx_note")
        source = customer.get("frontend")
        feCustomerId = customer.get("fe_customer_id")
        data = customer.get("data")
        data["bo_customer_id"] = customer.get("bo_customer_id")
        customerNumber = self.putItem(
            "customers",
            source=source,
            value=feCustomerId,
            data=data
        )
        self.logger.info("Customer created successfully, customer number: {customer_number}".format(
                customer_number=customerNumber
            )
        )
        return customerNumber

    def getTotalProductsCount(self, cutDt):
        return self.getCount(
            "products",
            source=self.setting["source"],
            updateDt=cutDt
        )

    def getProducts(self, cutDt, limit, offset):
        return self.getItems(
            "products",
            limit,
            offset,
            source=self.setting["source"],
            updateDt=cutDt
        )
