import traceback
from .mage2agency import Mage2Agency


class Mage2Agent(Mage2Agency):
    def __init__(self, setting=None, logger=None, feApp='mage2', boApp='mage2', dataWald=None, feConn=None, boConn=None):
        Mage2Agency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    def boProductsExtDataTotalFt(self, dataType, cutDt):
        EXPORTPRODUCTSCOUNTSQL = """
            SELECT count(*) AS total
            FROM catalog_product_entity a
            INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
            WHERE updated_at >= '{updated_at}' 
            AND b.attribute_set_name LIKE '{attribute_set_name}'
            AND a.sku like 'MCC%'
        """
        totalCount = 0
        if dataType == "imagegallery":
            totalCount = self.mage2.getTotalProductsCount(cutDt, sql=EXPORTPRODUCTSCOUNTSQL)
        return totalCount

    def setImageGallery(self, cutDt, offset=None, limit=None):
        EXPORTMEDIAIMAGESEESQL = """
            SELECT
            t0.sku,
            CONCAT('{base_url}', t1.value) as 'value',
            '{image_type}' as 'type'
            FROM
            catalog_product_entity t0,
            catalog_product_entity_varchar t1,
            eav_attribute t2
            WHERE t0.row_id = t1.row_id
            AND t0.sku like 'MCC%'
            AND t1.attribute_id = t2.attribute_id
            AND t2.attribute_code = '{attribute_code}'
            AND t0.updated_at >= '{updated_at}'
        """

        EXPORTMEDIAGALLERYEESQL = """
            SELECT
            t0.sku,
            CONCAT('{base_url}', t1.value) as 'value',
            t2.store_id,
            t2.position,
            t2.label,
            'mage2' as 'media_source',
            'media_gallery' as 'type'
            FROM
            catalog_product_entity t0,
            catalog_product_entity_media_gallery t1,
            catalog_product_entity_media_gallery_value t2,
            catalog_product_entity_media_gallery_value_to_entity t3
            WHERE t0.row_id = t3.row_id
            AND t0.sku like 'MCC%'
            AND t1.value_id = t2.value_id
            AND t1.value_id = t3.value_id
            AND t0.updated_at >= '{updated_at}'
        """
        try:
            rawDataImage = self.mage2.getImages(cutDt, offset=offset, limit=limit, sql=EXPORTMEDIAIMAGESEESQL)
            rawDataGallery = self.mage2.getGallery(cutDt, offset=offset, limit=limit, sql=EXPORTMEDIAGALLERYEESQL)
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
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def boProductsExtDataExtFt(self, dataType, productsExtData, rawProductsExtData):
        if dataType == "imagegallery":
            for i in reversed(range(0, len(productsExtData))):
                for element in productsExtData[i]['data'].get('media_gallery'):
                    try:
                        element.get('label').encode('ascii')
                    except UnicodeEncodeError as e:
                        self.logger.info(element)
                        log = traceback.format_exc()
                        self.logger.exception(log)
                        element.update({'label': '####'})
                    else:
                        pass  # string is ascii
        else:
            pass
