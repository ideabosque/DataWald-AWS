from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
import copy, json, traceback
from .txmap import TXMAP

class Mage2Agency(FrontEnd, BackOffice):
    def __init__(self, setting=None, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        self.map = TXMAP
        if setting is not None:
            self.map = setting.get("TXMAP") if setting.get("TXMAP") is not None else TXMAP
        if feConn is not None:
            self.mage2 = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.mage2 = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

    def __del__(self):
        self.mage2.__del__()

    @property
    def custOptValidation(self):
        if self.setting is not None and "CUSTOPTVALIDATION" in self.setting.keys():
            return self.setting["CUSTOPTVALIDATION"]
        else:
            return super(Mage2Agency, self).custOptValidation

    @property
    def custOptValValidation(self):
        if self.setting is not None and "CUSTOPTVALVALIDATION" in self.setting.keys():
            return self.setting["CUSTOPTVALVALIDATION"]
        else:
            return super(Mage2Agency, self).custOptValValidation

    def setImageGallery(self, cutDt, offset=None, limit=None):
        try:
            rawDataImage = self.mage2.getImages(cutDt, offset=offset, limit=limit)
            rawDataGallery = self.mage2.getGallery(cutDt, offset=offset, limit=limit)
            metaData = dict((k, list(set(map(lambda d: d[k], rawDataImage)))) for k in ['sku'])

            productsExtData = {}
            for sku in metaData["sku"]:
                rows = list(filter(lambda t: (t["sku"]==sku), rawDataImage))
                imageGallery = {}
                for row in rows:
                    imageGallery[row["type"]] = row["value"]

                imageGallery["media_gallery"] = [
                    {
                        "label": row["label"] if row["label"] is not None else "####",
                        "position": row["position"] if row["position"] is not None else 1,
                        "store_id": row["store_id"],
                        "media_source": row["media_source"],
                        "media_type": row["type"],
                        "value": row["value"]
                    } for row in list(filter(lambda t: (t["sku"]==sku), rawDataGallery))
                ]
                productsExtData[sku] = imageGallery
            return productsExtData
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def feOrdersFt(self, cutdt):
        rawOrders = self.mage2.getOrders(cutdt)
        orders = [
            self.transformData(order, self.map["order"]) for order in rawOrders
        ]
        return (orders, rawOrders)

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    # Sync Products from BackOffice to FrontEnd.
    def syncProductFt(self, product):
        sku = product['sku']
        attributeSet = product['table']
        data = product['data']
        data['status'] = data.pop('status', '1')
        data['visibility'] = data.pop('visibility', '4')
        data['tax_class_id'] = data.pop('tax_class_id', '2')
        typeId = data.pop('type_id', 'simple')
        storeId = data.pop('store_id', '0')
        try:
            product['fe_product_id'] = self.mage2.syncProduct(sku, attributeSet, data, typeId, storeId)
        except Exception:
            raise

    def syncProductExtDataFt(self, productExtData):
        sku = productExtData['sku']
        dataType = productExtData['data_type']
        data = productExtData['data']
        try:
            productExtData['fe_product_id'] = self.mage2.syncProductExtData(sku, dataType, data)
        except Exception:
            raise

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        productsExtData = []
        rawProductsExtData = {}
        if dataType == "imagegallery":
            rawProductsExtData = self.setImageGallery(cutDt,offset,limit)
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

    def boProductsExtDataTotalFt(self, dataType, cutDt):
        totalCount = 0
        if dataType == "imagegallery":
            totalCount = self.mage2.getTotalProductsCount(cutDt)
        return totalCount
