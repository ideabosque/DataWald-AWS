from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
from copy import deepcopy
from decimal import Decimal
import traceback, json

class SQSAgency(FrontEnd, BackOffice):
    def __init__(self, setting={}, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        if feConn is not None:
            self.sqs = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.sqs = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)
        self.inventory = {
            "store_id": 0,
            "warehouse": None,
            "on_hand": 0,
            "past_on_hand": 0,
            "qty": 0,
            "full": True,
            "in_stock": False
        }
        self.imageGallery = {
            "image": None,
            "small_image": None,
            "thumbnail": None,
            "swatch_image": None,
            "media_gallery": []
        }
        self.mediaGallery = {
            "value": None,
            "store_id": 0,
            "position": 1,
            "label": None,
            "media_source" : 'SQS',
            "media_type" : "image"
        }

    def setRawData(self, **params):
        queueName = params.pop("queueName", None)
        self.rawData = self.sqs.getData(queueName)

    def setInventory(self, rawData):
        try:
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in ['sku', 'warehouse'])
            for sku in metaData["sku"]:
                inventories = []
                for warehouse in metaData["warehouse"]:
                    rows = list(filter(lambda t: (t["sku"]==sku and t["warehouse"]==warehouse), rawData))
                    if len(rows) != 0:
                        entity = rows[0]
                        inventory = deepcopy(self.inventory)
                        inventory["warehouse"] = warehouse
                        inventory["qty"] = Decimal(entity["qty"])
                        if "store_id" in entity.keys():
                            inventory["store_id"] = entity["store_id"]
                        if "full" in entity.keys():
                            inventory["full"] = bool(entity["full"])
                        if inventory["full"]:
                            inventory["on_hand"] = inventory["qty"]
                        inventory["in_stock"] = True if inventory["qty"] > 0 else False
                        inventories.append(inventory)
                productsExtData[sku] = inventories
            return productsExtData
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def setImageGallery(self, rawData):
        try:
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in ['sku'])
            for sku in metaData["sku"]:
                imageGallery = deepcopy(self.imageGallery)
                rows = list(filter(lambda t: (t["sku"]==sku), rawData))
                uniqueValues = []
                for row in rows:
                    mediaGallery = deepcopy(self.mediaGallery)
                    for key in mediaGallery.keys():
                        if key in row.keys():
                            mediaGallery[key] = row[key]
                    for k, v in imageGallery.items():
                        if type(v) is list and row["value"] not in uniqueValues:
                        #if k == 'media_gallery' and row["value"] not in uniqueValues:
                            imageGallery[k].append(mediaGallery)
                            uniqueValues.append(row['value'])
                        elif k == row["type"] and type(v) is not list:
                            imageGallery[k] = row["value"]

                # Assign image to the value if it is None.
                for k, v in imageGallery.items():
                    if k != "media_gallery" and v is None and len(imageGallery["media_gallery"]) > 0:
                        imageGallery[k] = imageGallery["media_gallery"][0]["value"]

                productsExtData[sku] = imageGallery
            return productsExtData
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def feOrdersFt(self, cutdt):
        orders = []
        for _order in self.rawData:
            order = {}
            for k, v in _order.items():
                v = "####" if v == "" else v
                if k == "order_id":
                    order["fe_order_id"] = v
                elif k == "order_date":
                    order["fe_order_date"] = v
                    if "order_update_date" not in _order.keys():
                        order["fe_order_update_date"] = v
                elif k == "order_update_date":
                    order["fe_order_update_date"] = v
                elif k == "order_status":
                    order["fe_order_status"] = v
                else:
                    order[k] = v
            orders.append(order)

        return (orders, self.rawData)

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    def boProductsFt(self, frontend, table, offset, limit, cutDt=None):
        """We could add validation into this function.
        """
        products = []
        headers = self.getMetadata(frontend, table)
        metadatas = dict(
            [(metadata["dest"], {"src": metadata["src"], "funct": metadata["funct"]}) for metadata in [header["metadata"] for header in headers]]
        )

        for rawProduct in self.rawData:
            try:
                data = self.transformData(rawProduct, metadatas)
                product = {}
                product["sku"] = data['sku']
                product["frontend"] = frontend
                product["table"] = table
                product["raw_data"] = rawProduct
                product["data"] = data
                product["create_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                product["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                products.append(product)
            except Exception as e:
                self.logger.error(data)
                self.logger.error(rawProduct)
                log = traceback.format_exc()
                self.logger.exception(log)
                # raise
        return (products, self.rawData)

    def boProductsExtFt(self, products, rawProducts):
        pass

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        productsExtData = []
        rawProductsExtData = {}
        if dataType == "inventory":
            rawProductsExtData = self.setInventory(self.rawData)
        elif dataType == "imagegallery":
            rawProductsExtData = self.setImageGallery(self.rawData)
        else:
            raise Exception("Data Type({0}) is not supported.".format(dataType))

        for sku, data in rawProductsExtData.items():
            try:
                productExtData = {}
                productExtData["sku"] = sku
                productExtData["frontend"] = frontend
                productExtData["data_type"] = dataType
                productExtData["data"] = data
                productExtData["create_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                productExtData["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                productsExtData.append(productExtData)
            except Exception as e:
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
        return (productsExtData, self.rawData)

    def boProductsExtDataExtFt(self, dataType, ProductsExtData, rawProductsExtData):
        pass

    def feCustomersFt(self, cutDt):
        customers = [
            {
                "fe_customer_id": _customer["email"],
                "data": _customer,
                "create_dt": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "update_dt": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")               
            } for _customer in self.rawData
        ]
        return (customers, self.rawData)

    def feCustomersExtFt(self, customers, rawCustomers):
        pass
