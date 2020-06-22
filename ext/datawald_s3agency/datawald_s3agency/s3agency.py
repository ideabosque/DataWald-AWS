from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
from copy import deepcopy
from decimal import Decimal
import traceback, json, uuid

class S3Agency(FrontEnd, BackOffice):
    def __init__(self, setting={}, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        self.map = setting.get("TXMAP", None)
        if feConn is not None:
            self.s3 = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.s3 = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

        self.option = {
            "title": None,
            "title_alt": None,
            "store_id": 0,
            "type": None,
            "is_require": None,
            "sort_order": 1,
            "option_sku": None,
            "option_price": None,
            "option_price_type": None,
            "option_values": []
        }
        self.optionValue = {
            "option_value_title": None,
            "option_value_title_alt": None,
            "store_id": 0,
            "option_value_sku": None,
            "option_value_price": None,
            "option_value_price_type": None,
            "option_value_sort_order": 1
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
            "media_source" : 'S3',
            "media_type" : "image"
        }
        self.links = {
            "cross_sell": [],
            "up_sell": [],
            "relation": []
        }
        self.link = {
            "linked_sku": None,
            "position": 0
        }
        self.category = {
            "store_id": 0,
            "delimiter": "/",
            "apply_all_levels": True,
            "path": None,
            "position": 0
        }
        self.inventory = {
            "store_id": 0,
            "warehouse": None,
            "on_hand": 0,
            "past_on_hand": 0,
            "qty": 0,
            "full": True,
            "in_stock": False
        }

    def setRawData(self, **params):
        bucket = params.pop("bucket")
        key = params.pop("key")
        newLine = params.pop("newLine", "\r")
        self.rawData = self.s3.getRows(bucket, key, newLine=newLine)

    def setCustomOption(self, rawData):
        try:
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in ['sku', 'title'])
            for sku in metaData["sku"]:
                options = []
                for title in metaData["title"]:
                    rows = list(filter(lambda t: (t["sku"]==sku and t["title"]==title), rawData))
                    if len(rows) != 0:
                        option = deepcopy(self.option)
                        for row in rows:
                            for k, v in option.items():
                                if type(v) is list:
                                    optionValue = deepcopy(self.optionValue)
                                    for x in optionValue.keys():
                                        optionValue[x] = row[x] if x in row.keys() else optionValue[x]

                                    optionValue = {k: v for k, v in optionValue.items() if v is not None and v != ""}
                                    if "option_value_title" in optionValue.keys():
                                        option[k].append(optionValue)
                                else:
                                    option[k] = row[k] if k in row.keys() and row[k] is not None else option[k]

                        option = {k: v for k, v in option.items() if v is not None and v != ""}
                        options.append(option)
                productsExtData[sku]= options
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

    def setLinks(self, rawData):
        try:
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in ['sku'])
            for sku in metaData["sku"]:
                links = deepcopy(self.links)
                for key in links.keys():
                    rows = list(filter(lambda t: (t["sku"]==sku and t["type"]==key), rawData))
                    for row in rows:
                        link = deepcopy(self.link)
                        for k,v in link.items():
                            link[k] = row[k]
                        links[key].append(link)
                productsExtData[sku] = links
            return productsExtData
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def setCategories(self, rawData):
        try:
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in ['sku'])
            for sku in metaData["sku"]:
                categories = []
                rows = list(filter(lambda t: t["sku"]==sku, rawData))
                for row in rows:
                    category = deepcopy(self.category)
                    for key in category.keys():
                        category[key] = row.pop(key, category[key])
                    categories.append(category)
                productsExtData[sku] = categories
            return productsExtData
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

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
                log = traceback.format_exc()
                self.logger.exception(log)
                self.logger.error(data)
                self.logger.error(rawProduct)
                # raise
        return (products, self.rawData)

    def boProductsExtFt(self, products, rawProducts):
        pass

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        productsExtData = []
        rawProductsExtData = {}
        if dataType == "customoption":
            rawProductsExtData = self.setCustomOption(self.rawData)
        elif dataType == "imagegallery":
            rawProductsExtData = self.setImageGallery(self.rawData)
        elif dataType == "links":
            rawProductsExtData = self.setLinks(self.rawData)
        elif dataType == "categories":
            rawProductsExtData = self.setCategories(self.rawData)
        elif dataType == "inventory":
            rawProductsExtData = self.setInventory(self.rawData)
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

    # Sync PurchaseOrders from BackOffice to FrontEnd.
    def syncPurchaseOrderFt(self, purchaseOrder):
        key = "PurchaseOrder/{}".format(purchaseOrder["bo_po_num"])
        self.s3.putObj(key, purchaseOrder)
        purchaseOrder["fe_po_num"] = uuid.uuid1().int>>64

    # Sync ItemReceipts from BackOffice to FrontEnd.
    def feItemReceiptsFt(self, cutDt=None):
        folder = "ItemReceipt"
        rawItemReceipts = self.s3.getObjs(folder, limit=10)
        itemReceipts = []
        for rawItemReceipt in rawItemReceipts:
            itemReceipt = {}
            itemReceipt["bo_po_num"] = rawItemReceipt["key"]
            itemReceipt["data"] = rawItemReceipt
            _lastModified = rawItemReceipt.pop("lastModified")
            itemReceipt["create_dt"] = _lastModified
            itemReceipt["update_dt"] = _lastModified
            itemReceipts.append(itemReceipt)
        return (itemReceipts, rawItemReceipts)

    def feItemReceiptsExtFt(self, itemReceipts, rawItemReceipts):
        pass

    def boOrderFt(self, order):
        # Transfer the data format.
        self.logger.info(order)
        order = self.transformData(order, self.map["salesOrder"])
        self.logger.info(order)
        return order

    # Sync Orders from Frontend to BackOffice.
    def insertOrdersFt(self, newOrders):
        boOrders = self.s3.insertOrders(newOrders)
        for boOrder in boOrders:
            if boOrder['tx_status'] == 'F':
                log = "Fail to insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.error(log)
                self.logger.error(boOrder['tx_note'])
            else:
                log = "Successfully insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.info(log)
        return boOrders