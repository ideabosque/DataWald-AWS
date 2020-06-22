import re, copy
from pymysql import connect, cursors

class Adaptor(object):
    """Adaptor contain MySQL cursor object.
    """

    def __init__(self, **params):
        self._mySQLConn = connect(
            host=params.get("host"),
            user=params.get("user"),
            password=params.get("password"),
            db=params.get("db"),
            port=params.get("port", 3306),
            charset='utf8mb4',
            cursorclass=cursors.DictCursor
        )
        self._mySQLCursor = self._mySQLConn.cursor()

    def __del__(self):
        if self._mySQLConn.open:
            self.disconnect()

    @property
    def mySQLCursor(self):
        """MySQL Server cursor object.
        """
        return self._mySQLCursor

    def disconnect(self):
        self._mySQLConn.close()

    def rollback(self):
        self._mySQLConn.rollback()

    def commit(self):
        self._mySQLConn.commit()

class Mage2Connector(object):
    """Magento 2 connection with functions.
    """

    GETPRODUCTIDBYSKUSQL = """SELECT distinct entity_id FROM catalog_product_entity WHERE sku = %s;"""

    GETROWIDBYENTITYIDSQL = """SELECT distinct row_id FROM catalog_product_entity WHERE entity_id = %s;"""

    ENTITYMETADATASQL = """
        SELECT eet.entity_type_id, eas.attribute_set_id
        FROM eav_entity_type eet, eav_attribute_set eas
        WHERE eet.entity_type_id = eas.entity_type_id
        AND eet.entity_type_code = %s
        AND eas.attribute_set_name = %s;"""

    ATTRIBUTEMETADATASQL = """
        SELECT DISTINCT t1.attribute_id, t2.entity_type_id, t1.backend_type, t1.frontend_input
        FROM eav_attribute t1, eav_entity_type t2
        WHERE t1.entity_type_id = t2.entity_type_id
        AND t1.attribute_code = %s
        AND t2.entity_type_code = %s;"""

    ISENTITYEXITSQL = """SELECT count(*) as count FROM {entityTypeCode}_entity WHERE entity_id = %s;"""

    ISATTRIBUTEVALUEEXITSQL = """
        SELECT count(*) as count
        FROM {entityTypeCode}_entity_{dataType}
        WHERE attribute_id = %s
        AND store_id = %s
        AND {key} = %s;"""

    REPLACEATTRIBUTEVALUESQL = """REPLACE INTO {entityTypeCode}_entity_{dataType} ({cols}) values ({vls});"""

    UPDATEENTITYUPDATEDATSQL = """UPDATE {entityTypeCode}_entity SET updated_at = UTC_TIMESTAMP() WHERE entity_id = %s;"""

    GETOPTIONIDSQL = """
        SELECT t2.option_id
        FROM eav_attribute_option t1, eav_attribute_option_value t2
        WHERE t1.option_id = t2.option_id
        AND t1.attribute_id = %s
        AND t2.value = %s
        AND t2.store_id = %s;"""

    INSERTCATALOGPRODUCTENTITYEESQL = """
        INSERT INTO catalog_product_entity
        (entity_id, created_in, updated_in, attribute_set_id, type_id, sku, has_options, required_options, created_at, updated_at)
        VALUES(0, 1, 2147483647, %s, %s, %s, 0, 0, UTC_TIMESTAMP(), UTC_TIMESTAMP());"""

    INSERTCATALOGPRODUCTENTITYSQL = """
        INSERT INTO catalog_product_entity
        (attribute_set_id, type_id, sku, has_options, required_options, created_at, updated_at)
        VALUES(%s, %s, %s, 0, 0, UTC_TIMESTAMP(), UTC_TIMESTAMP());"""

    UPDATECATALOGPRODUCTSQL = """
        UPDATE catalog_product_entity
        SET attribute_set_id = %s,
        type_id = %s,
        updated_at = UTC_TIMESTAMP()
        WHERE {key} = %s;"""

    INSERTEAVATTRIBUTEOPTIONSQL = """INSERT INTO eav_attribute_option (attribute_id) VALUES (%s);"""

    OPTIONVALUEEXISTSQL = """
        SELECT COUNT(*) as cnt FROM eav_attribute_option_value
        WHERE option_id = %s
        AND store_id = %s;"""

    INSERTOPTIONVALUESQL = """INSERT INTO eav_attribute_option_value (option_id, store_id, value) VALUES (%s, %s, %s);"""

    UPDATEOPTIONVALUESQL = """UPDATE eav_attribute_option_value SET value = %s WHERE option_id = %s AND store_id = %s;"""

    GETCUSTOMOPTIONIDBYTITLESQL = """
        SELECT a.option_id
        FROM catalog_product_option a
        INNER JOIN catalog_product_option_title b ON a.option_id = b.option_id
        WHERE a.product_id = %s AND b.title = %s;"""

    REPLACECUSTOMOPTIONSQL = """REPLACE INTO catalog_product_option ({optCols}) VALUES ({optVals});"""

    INSERTCUSTOMOPTIONTITLESQL = """
        INSERT INTO catalog_product_option_title
        (option_id, store_id, title)
        VALUES (%s,%s,%s) on DUPLICATE KEY UPDATE title = %s;"""

    INSERTCUSTOMOPTIONPRICESQL = """
        INSERT INTO catalog_product_option_price
        (option_id, store_id, price, price_type)
        VALUES (%s,%s,%s,%s) on DUPLICATE KEY UPDATE price = %s, price_type = %s;"""

    GETCUSTOMOPTIONTYPEIDBYTITLESQL = """
        SELECT a.option_type_id
        FROM catalog_product_option_type_value a
        INNER JOIN catalog_product_option_type_title b on a.option_type_id = b.option_type_id
        WHERE a.option_id = %s
        AND b.title = %s;"""

    REPLACECUSTOMOPTIONTYPEVALUESQL = """REPLACE INTO catalog_product_option_type_value ({optValCols}) VALUES ({optValVals});"""

    INSERTCUSTOMOPTIONTYPETITLESQL = """
        INSERT INTO catalog_product_option_type_title
        (option_type_id, store_id, title)
        VALUES (%s,%s,%s) on DUPLICATE KEY UPDATE title = %s;"""

    INSERTCUSTOMOPTIONTYPEPRICESQL = """
        INSERT INTO catalog_product_option_type_price
        (option_type_id, store_id, price, price_type)
        VALUES (%s,%s,%s,%s) on DUPLICATE KEY UPDATE price = %s, price_type = %s;"""

    DELETECUSTOMOPTIONSQL = """
        DELETE FROM catalog_product_option WHERE product_id = %s"""

    UPDATEPRODUCTHASOPTIONSSQL = """
        UPDATE catalog_product_entity SET has_options = 1 WHERE entity_id = %s;"""

    INSERTPRODUCTIMAGEGALLERYSQL = """
        INSERT INTO catalog_product_entity_media_gallery
        (attribute_id,value,media_type) VALUES (%s,%s,%s);"""

    INSERTPRODUCTIMAGEGALLERYEXTSQL = """
        INSERT INTO catalog_product_entity_media_gallery_ext
        (value_id,media_source,file) VALUES (%s,%s,%s);"""

    INSERTPRODUCTIMAGEGALLERYVALUESQL = """
        INSERT INTO catalog_product_entity_media_gallery_value ({cols}) VALUES ({vals});"""

    INSERTMEDIAVALUETOENTITYSQL = """
        INSERT IGNORE INTO catalog_product_entity_media_gallery_value_to_entity ({cols}) VALUES ({vals});"""

    INSERTPRODUCTIMAGESQL = """
        INSERT INTO catalog_product_entity_varchar ({cols}) VALUES ({vals}) ON DUPLICATE KEY UPDATE value = %s;"""

    SELECTLINKTYPEIDSQL = """
        SELECT link_type_id FROM catalog_product_link_type WHERE code = %s;"""

    SELECTLINKATTSQL = """
        SELECT
        t0.link_type_id,
        t1.product_link_attribute_id
        FROM
        catalog_product_link_type t0,
        catalog_product_link_attribute t1
        WHERE t0.link_type_id = t1.link_type_id
        AND t1.product_link_attribute_code = "position"
        AND t0.code = %s;"""

    INSERTCATALOGPRODUCTLINKSQL = """
        INSERT IGNORE INTO catalog_product_link (product_id,linked_product_id,link_type_id) VALUES (%s,%s,%s);"""

    INSERTCATALOGPRODUCTLINKATTRIBUTEINT = """
        INSERT IGNORE INTO catalog_product_link_attribute_int (product_link_attribute_id,link_id,value) VALUES (%s,%s,%s);"""

    DELETEPRODUCTLINKSQL = """
        DELETE FROM catalog_product_link WHERE product_id = %s and link_type_id = %s"""

    DELETEPRODUCTIMAGEGALLERYSQL = """
        DELETE a
        FROM catalog_product_entity_media_gallery a
        INNER JOIN catalog_product_entity_media_gallery_value b ON a.value_id = b.value_id
        WHERE b.entity_id = %s"""

    DELTEEPRODUCTIMAGEEXTSQL = """
        DELETE FROM catalog_product_entity_media_gallery_ext WHERE file IN (%s)"""

    DELETEPRODUCTIMAGEGALLERYEXTSQL = """
        DELETE a
        FROM catalog_product_entity_media_gallery_ext a
        INNER JOIN catalog_product_entity_media_gallery_value b ON a.value_id = b.value_id
        WHERE b.entity_id = %s"""

    GETPRODUCTOUTOFSTOCKQTYSQL = """
        SELECT min_qty
        FROM cataloginventory_stock_item
        WHERE product_id = %s AND stock_id = %s AND use_config_min_qty = 0;"""

    SETSTOCKSTATUSQL = """
        INSERT INTO cataloginventory_stock_status
        (product_id,website_id,stock_id,qty,stock_status)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        qty = %s,
        stock_status = %s;"""

    SETSTOCKITEMSQL = """
        INSERT INTO  cataloginventory_stock_item
        (product_id,stock_id,qty,is_in_stock,website_id)
        VALUES (%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        qty = %s,
        is_in_stock = %s;"""

    SETPRODUCTCATEGORYSQL = """
        INSERT INTO catalog_category_product
        (category_id, product_id, position)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        category_id = %s,
        product_id = %s,
        position = %s;"""

    UPDATECATEGORYCHILDRENCOUNTSQL = """
        UPDATE catalog_category_entity
        SET children_count = children_count + 1
        where entity_id = %s;"""

    UPDATECATEGORYCHILDRENCOUNTEESQL = """
        UPDATE catalog_category_entity
        SET children_count = children_count + 1
        where row_id = %s;"""

    GETMAXCATEGORYIDSQL = """
        SELECT max(entity_id) as max_category_id FROM catalog_category_entity;"""

    # for Magento 2 CE
    GETCATEGORYIDBYATTRIBUTEVALUEANDPATHSQL = """
        SELECT a.entity_id
        FROM catalog_category_entity a
        INNER JOIN catalog_category_entity_varchar b ON a.entity_id = b.entity_id
        INNER JOIN eav_attribute c ON b.attribute_id = c.attribute_id AND c.attribute_code = 'name' and c.entity_type_id = 3
        WHERE a.level = %s and a.parent_id = %s and b.value = %s;"""

    # for Magento 2 EE
    GETCATEGORYIDBYATTRIBUTEVALUEANDPATHEESQL = """
        SELECT a.row_id
        FROM catalog_category_entity a
        INNER JOIN catalog_category_entity_varchar b ON a.row_id = b.row_id
        INNER JOIN eav_attribute c ON b.attribute_id = c.attribute_id AND c.attribute_code = 'name' and c.entity_type_id = 3
        WHERE a.level = %s and a.parent_id = %s and b.value = %s;"""

    # for Magento 2 CE
    INSERTCATALOGCATEGORYENTITYSQL = """
        INSERT INTO catalog_category_entity
        (attribute_set_id, parent_id, created_at, updated_at, path, level, children_count, position)
        VALUES (%s, %s, now(), now(), %s, %s, %s, %s);"""

    # for Magento 2 EE
    INSERTCATALOGCATEGORYENTITYEESQL = """
        INSERT INTO catalog_category_entity
        (entity_id, created_in, updated_in, attribute_set_id, parent_id, created_at, updated_at, path, level, children_count,position)
        VALUES (%s, 1, 2147483647, %s, %s, now(), now(), %s, %s, %s,%s);"""

    EXPORTPRODUCTSCOUNTSQL = """
        SELECT count(*) AS total
        FROM catalog_product_entity a
        INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
        WHERE updated_at >= '{updated_at}' AND b.attribute_set_name LIKE '{attribute_set_name}'
    """

    # for Magento 2 CE
    EXPORTMEDIAIMAGESSQL = """
        SELECT
        t0.sku,
        CONCAT('{base_url}', t1.value) as 'value',
        '{image_type}' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_varchar t1,
        eav_attribute t2
        WHERE t0.entity_id = t1.entity_id
        AND t1.attribute_id = t2.attribute_id
        AND t2.attribute_code = '{attribute_code}'
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 EE
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
        AND t1.attribute_id = t2.attribute_id
        AND t2.attribute_code = '{attribute_code}'
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 CE
    EXPORTMEDIAGALLERYSQL = """
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
        WHERE t0.entity_id = t3.entity_id
        AND t1.value_id = t2.value_id
        AND t1.value_id = t3.value_id
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 EE
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
        AND t1.value_id = t2.value_id
        AND t1.value_id = t3.value_id
        AND t0.updated_at >= '{updated_at}'
    """

    # for Magento 2 CE/EE
    GETCONFIGSIMPLEPRODUCTSSQL = """
        SELECT 
        t4.parent_id, 
        t5.attribute_id, 
        t4.product_id as child_id, 
        t3.value, 
        t0.sku
        FROM 
        catalog_product_entity t0,
        catalog_product_entity_int t1,
        eav_attribute_option t2,
        eav_attribute_option_value t3,
        catalog_product_super_link t4,
        catalog_product_super_attribute t5
        WHERE t0.{id} = t1.{id}
        AND t1.attribute_id = t2.attribute_id AND t1.value = t2.option_id
        AND t2.option_id = t3.option_id AND t3.store_id = {store_id}
        AND t4.parent_id = t5.product_id
        AND t0.entity_id = t4.product_id
        AND t5.attribute_id = t1.attribute_id AND t1.store_id = {store_id}
        AND t5.product_id = {product_id} AND t5.attribute_id IN ({attribute_ids})
    """

    REPLACECATALOGPRODUCTRELATIONSQL = """
        REPLACE INTO catalog_product_relation
        (parent_id, child_id)
        VALUES ({parent_id}, {child_id})
    """

    REPLACECATALOGPRODUCTSUPERLINKSQL = """
        REPLACE INTO catalog_product_super_link
        (product_id, parent_id)
        VALUES ({product_id}, {parent_id})
    """

    REPLACECATALOGPRODUCTSUPERATTRIBUTESQL = """
        INSERT IGNORE INTO catalog_product_super_attribute
        (product_id, attribute_id)
        VALUES ({product_id}, {attribute_id})
    """

    UPDATEPRODUCTVISIBILITYSQL = """
        UPDATE catalog_product_entity_int SET value = {value}
        WHERE {id} = {product_id} AND
        attribute_id in (SELECT attribute_id FROM eav_attribute WHERE entity_type_id = 4 AND attribute_code = 'visibility')
    """

    GETORDERSSQL = """
        SELECT
        sales_order.entity_id AS id,
        sales_order.increment_id AS m_order_inc_id,
        sales_order.created_at AS m_order_date,
        sales_order.updated_at AS m_order_update_date,
        sales_order.status AS m_order_status,
        customer_group.customer_group_code AS m_customer_group,
        sales_order.store_id AS m_store_id,
        sales_order.customer_id AS m_customer_id,
        '' AS shipment_carrier,
        IFNULL(sales_order.shipping_method,"") AS shipment_method,
        IFNULL(bill_to.firstname,'') AS billto_firstname,
        IFNULL(bill_to.lastname,'') AS billto_lastname,
        IFNULL(bill_to.email,'') AS billto_email,
        IFNULL(bill_to.company,'') AS billto_companyname,
        IFNULL(bill_to.street,'') AS billto_address,
        IFNULL(bill_to.city,'') AS billto_city,
        IFNULL(bill_to_region.code,'') AS billto_region,
        IFNULL(bill_to.country_id,'') AS billto_country,
        IFNULL(bill_to.postcode,'') AS billto_postcode,
        IFNULL(bill_to.telephone,'') AS billto_telephone,
        IFNULL(ship_to.firstname,'') AS shipto_firstname,
        IFNULL(ship_to.lastname,'') AS shipto_lastname,
        IFNULL(ship_to.company,'') AS shipto_companyname,
        IFNULL(ship_to.street,'') AS shipto_address,
        IFNULL(ship_to.city,'') AS shipto_city,
        IFNULL(ship_to_region.code,'') AS shipto_region,
        IFNULL(ship_to.country_id,'') AS shipto_country,
        IFNULL(ship_to.postcode,'') AS shipto_postcode,
        IFNULL(ship_to.telephone,'') AS shipto_telephone,
        IFNULL(sales_order.total_qty_ordered,0) AS total_qty,
        IFNULL(sales_order.subtotal,0) AS sub_total,
        IFNULL(sales_order.discount_amount,0) AS discount_amt,
        IFNULL(sales_order.shipping_amount,0) AS shipping_amt,
        IFNULL(sales_order.tax_amount,0) AS tax_amt,
        '0' AS giftcard_amt,
        '0' AS storecredit_amt,
        sales_order.grand_total AS grand_total,
        sales_order.coupon_code AS coupon_code,
        IFNULL(sales_order.shipping_tax_amount,0) AS shipping_tax_amt,
        'checkmo' AS payment_method
        FROM
        sales_order
        LEFT JOIN sales_order_address bill_to on (sales_order.entity_id = bill_to.parent_id and bill_to.address_type = 'billing')
        LEFT JOIN sales_order_address ship_to on (sales_order.entity_id = ship_to.parent_id and ship_to.address_type = 'shipping')
        LEFT JOIN directory_country_region bill_to_region on (bill_to.region_id = bill_to_region.region_id and bill_to.country_id = bill_to_region.country_id)
        LEFT JOIN directory_country_region ship_to_region on (ship_to.region_id = ship_to_region.region_id and ship_to.country_id = ship_to_region.country_id)
        LEFT JOIN customer_entity customer on sales_order.customer_id = customer.entity_id
        LEFT JOIN customer_group customer_group on customer.group_id = customer_group.customer_group_id
        WHERE sales_order.updated_at > '{updated_at}'
        ORDER BY sales_order.entity_id
    """

    GETORDERITEMSSQL = """
        SELECT
        sales_order_item.item_id AS id,
        sales_order_item.order_id AS m_order_id,
        sales_order_item.sku AS sku,
        sales_order_item.name AS name,
        '' AS uom,
        sales_order_item.original_price AS original_price,
        sales_order_item.price AS price,
        sales_order_item.discount_amount AS discount_amt,
        sales_order_item.tax_amount AS tax_amt,
        sales_order_item.qty_ordered AS qty,
        sales_order_item.row_total AS sub_total
        FROM
        sales_order_item
        WHERE parent_item_id is null 
        AND order_id = '{order_id}'
    """



    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger
        self._adaptor = None

    def __del__(self):
        if self._adaptor is not None:
            self.adaptor.__del__()
            self.logger.info("Close Mage2 DB connection")

    def connect(self):
        """Initiate the connect with MySQL server.
        """
        params = {
            "host": self.setting["MAGE2DBSERVER"],
            "user": self.setting["MAGE2DBUSERNAME"],
            "password": self.setting["MAGE2DBPASSWORD"],
            "db": self.setting["MAGE2DB"],
            "port": self.setting["MAGE2DBPORT"]
        }
        self.logger.info("Open Mage2 DB connection")
        return Adaptor(**params)

    @property
    def adaptor(self):
        self._adaptor = self.connect() if self._adaptor is None else self._adaptor
        return self._adaptor

    def getEntityMetaData(self, entityTypeCode='catalog_product', attributeSet='Default'):
        self.adaptor.mySQLCursor.execute(self.ENTITYMETADATASQL, [entityTypeCode, attributeSet])
        entityMetadata = self.adaptor.mySQLCursor.fetchone()
        if entityMetadata is not None:
            return entityMetadata
        else:
            log = "attribute_set/entity_type_code: {0}/{1} not existed".format(attributeSet, entityTypeCode)
            raise Exception(log)

    def getAttributeMetadata(self, attributeCode, entityTypeCode):
        self.adaptor.mySQLCursor.execute(self.ATTRIBUTEMETADATASQL, [attributeCode, entityTypeCode])
        attributeMetadata = self.adaptor.mySQLCursor.fetchone()
        if attributeMetadata is None:
            log = "Entity Type/Attribute Code: {0}/{1} does not exist".format(entityTypeCode, attributeCode)
            raise Exception(log)
        dataType = attributeMetadata['backend_type']
        return (dataType, attributeMetadata)

    def isEntityExit(self, entityTypeCode, entityId):
        sql = self.ISENTITYEXITSQL.format(entityTypeCode=entityTypeCode)
        self.adaptor.mySQLCursor.execute(sql, [entityId])
        exist = self.adaptor.mySQLCursor.fetchone()
        return exist['count']

    def isAttributeValueExit(self, entityTypeCode, dataType, attributeId, storeId, entityId):
        key = 'row_id' if self.setting['VERSION'] == "EE" else 'entity_id'
        sql = self.ISATTRIBUTEVALUEEXITSQL.format(entityTypeCode=entityTypeCode, dataType=dataType, key=key)
        self.adaptor.mySQLCursor.execute(sql, [attributeId, storeId, entityId])
        exist = self.adaptor.mySQLCursor.fetchone()
        return exist['count']

    def replaceAttributeValue(self, entityTypeCode, dataType, entityId, attributeId, value, storeId=0):
        if entityTypeCode == 'catalog_product' or entityTypeCode == 'catalog_category':
            cols = "entity_id, attribute_id, store_id, value"
            if self.setting['VERSION'] == "EE":
                cols = "row_id, attribute_id, store_id, value"
            vls = "%s, %s, {0}, %s".format(storeId)
            param = [entityId, attributeId, value]
        else:
            cols = "entity_id, attribute_id, value"
            vls = "%s, %s, %s"
            param = [entityId, attributeId, value]
        sql = self.REPLACEATTRIBUTEVALUESQL.format(entityTypeCode=entityTypeCode, dataType=dataType, cols=cols, vls=vls)
        self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.adaptor.mySQLCursor.execute(sql, param)
        self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    def updateEntityUpdatedAt(self, entityTypeCode, entityId):
        sql = self.UPDATEENTITYUPDATEDATSQL.format(entityTypeCode=entityTypeCode)
        self.adaptor.mySQLCursor.execute(sql, [entityId])

    def setAttributeOptionValues(self, attributeId, options, entityTypeCode="catalog_product", adminStoreId=0, updateExistingOption=False):
        optionId = self.getOptionId(attributeId, options[adminStoreId], adminStoreId)
        if optionId is None:
            self.adaptor.mySQLCursor.execute(self.INSERTEAVATTRIBUTEOPTIONSQL, [attributeId])
            optionId = self.adaptor.mySQLCursor.lastrowid
        for (storeId, optionValue) in options.items():
            self.adaptor.mySQLCursor.execute(self.OPTIONVALUEEXISTSQL, [optionId, storeId])
            exist = self.adaptor.mySQLCursor.fetchone()
            if not exist or exist['cnt'] == 0 :
                self.adaptor.mySQLCursor.execute(self.INSERTOPTIONVALUESQL, [optionId, storeId, optionValue])
            elif exist['cnt'] >0 and updateExistingOption == True:
                self.adaptor.mySQLCursor.execute(self.UPDATEOPTIONVALUESQL, [optionValue, optionId, storeId])
        return optionId

    def setMultiSelectOptionIds(self, attributeId, values, entityTypeCode="catalog_product", adminStoreId=0, delimiter="|"):
        values = values.strip('"').strip("'").strip("\n").strip()
        listValues = [v.strip() for v in values.split(delimiter)]
        listOptionIds = []
        for value in listValues:
            options = {0: value}
            optionId = self.setAttributeOptionValues(attributeId, options, entityTypeCode=entityTypeCode, adminStoreId=adminStoreId)
            listOptionIds.append(str(optionId))
        optionIds = ",".join(listOptionIds) if len(listOptionIds) > 0 else None
        return optionIds

    def getOptionId(self, attributeId, value, adminStoreId=0):
        self.adaptor.mySQLCursor.execute(self.GETOPTIONIDSQL, [attributeId, value, adminStoreId])
        res = self.adaptor.mySQLCursor.fetchone()
        optionId = None
        if res is not None:
            optionId = res['option_id']
        return optionId

    def getMultiSelectOptionIds(self, attributeId, values, adminStoreId=0, delimiter="|"):
        if values is None:
            return [None]
        values = values.strip('"').strip("'").strip("\n").strip()
        listValues = [v.strip() for v in values.split(delimiter)]
        listOptionIds = []
        for value in listValues:
            optionId = self.getOptionId(attributeId, value, adminStoreId=adminStoreId)
            listOptionIds.append(str(optionId))
        optionIds = ",".join(listOptionIds) if len(listOptionIds) > 0 else None
        return optionIds

    def getProductIdBySku(self, sku):
        self.adaptor.mySQLCursor.execute(self.GETPRODUCTIDBYSKUSQL, [sku])
        entity = self.adaptor.mySQLCursor.fetchone()
        if entity is not None:
            entityId = int(entity["entity_id"])
        else:
            entityId = 0
        return entityId

    def getRowIdByEntityId(self, entityId):
        self.adaptor.mySQLCursor.execute(self.GETROWIDBYENTITYIDSQL, [entityId])
        entity = self.adaptor.mySQLCursor.fetchone()
        if entity is not None:
            rowId = int(entity["row_id"])
        else:
            rowId = 0
        return rowId

    def insertCatalogProductEntity(self, sku, attributeSet='Default', typeId='simple'):
        entityMetadata = self.getEntityMetaData('catalog_product', attributeSet)
        if entityMetadata == None:
            return 0

        if self.setting['VERSION'] == 'EE':
            self.adaptor.mySQLCursor.execute("""SET FOREIGN_KEY_CHECKS = 0;""")
            self.adaptor.mySQLCursor.execute(self.INSERTCATALOGPRODUCTENTITYEESQL, (entityMetadata['attribute_set_id'], typeId, sku))
            productId = self.adaptor.mySQLCursor.lastrowid
            self.adaptor.mySQLCursor.execute("""UPDATE catalog_product_entity SET entity_id = row_id WHERE row_id = %s;""", (productId,))
            self.adaptor.mySQLCursor.execute("""INSERT INTO sequence_product (sequence_value) VALUES (%s);""", (productId,))
            self.adaptor.mySQLCursor.execute("""SET FOREIGN_KEY_CHECKS = 1;""")
        else:
            self.adaptor.mySQLCursor.execute(self.INSERTCATALOGPRODUCTENTITYSQL, (entityMetadata['attribute_set_id'], typeId, sku))
            productId = self.adaptor.mySQLCursor.lastrowid

        return productId

    def updateCatalogProductEntity(self, productId, attributeSet='Default', typeId='simple'):
        entityMetadata = self.getEntityMetaData('catalog_product', attributeSet)
        if entityMetadata == None:
            return 0
        key = 'row_id' if self.setting['VERSION'] == "EE" else 'entity_id'
        sql = self.UPDATECATALOGPRODUCTSQL.format(key=key)
        self.adaptor.mySQLCursor.execute(sql, [entityMetadata['attribute_set_id'], typeId, productId])

    def syncEntityData(self, entityId, data, entityTypeCode='catalog_product', storeId=0, adminStoreId=0):
        doNotUpdateOptionAttributes = ['status','visibility','tax_class_id']
        for attributeCode, value in data.items():
            (dataType, attributeMetadata) = self.getAttributeMetadata(attributeCode, entityTypeCode)
            if attributeMetadata['frontend_input'] == 'select' and attributeCode not in doNotUpdateOptionAttributes:
                optionId = self.getOptionId(attributeMetadata['attribute_id'], value, adminStoreId=adminStoreId)
                if optionId is None:
                    options = {0: value}
                    optionId = self.setAttributeOptionValues(attributeMetadata['attribute_id'], options, adminStoreId=adminStoreId)
                value = optionId
            elif attributeMetadata['frontend_input'] == 'multiselect':
                optionIds = self.getMultiSelectOptionIds(attributeMetadata['attribute_id'], value, adminStoreId=adminStoreId)
                if optionIds is None:
                    optionIds = self.setMultiSelectOptionIds(attributeMetadata['attribute_id'], value, adminStoreId=adminStoreId)
                value = optionIds

            # ignore the static datatype.
            if dataType != "static":
                exist = self.isAttributeValueExit(entityTypeCode, dataType, attributeMetadata['attribute_id'], adminStoreId, entityId)
                storeId = adminStoreId if exist == 0 else storeId
                self.replaceAttributeValue(entityTypeCode, dataType, entityId, attributeMetadata['attribute_id'], value, storeId=storeId)

    def _getWebsiteIdByStoreId(self, storeId):
        self.adaptor.mySQLCursor.execute("SELECT website_id FROM store WHERE store_id = %s",[storeId])
        res = self.adaptor.mySQLCursor.fetchone()
        websiteId = 0
        if res is not None:
            websiteId = res['website_id']
        return websiteId

    def assingWebsite(self, productId, storeId):
        websiteId = self._getWebsiteIdByStoreId(storeId)
        if websiteId == 0:
            websiteId = 1
        self.adaptor.mySQLCursor.execute("INSERT IGNORE INTO catalog_product_website (product_id, website_id) VALUES (%s, %s)",[productId,websiteId])

    def syncProduct(self, sku, attributeSet, data, typeId, storeId):
        try:
            productId = self.getProductIdBySku(sku)
            if productId == 0:
                productId = self.insertCatalogProductEntity(sku, attributeSet, typeId)
            else:
                self.updateCatalogProductEntity(productId, attributeSet, typeId)
            self.syncEntityData(productId, data, entityTypeCode='catalog_product', storeId=storeId)
            self.assingWebsite(productId,storeId)
            self.adaptor.commit()
            return productId
        except Exception:
            self.adaptor.rollback()
            raise

    def getCustomOption(self, productId, title):
        self.adaptor.mySQLCursor.execute(self.GETCUSTOMOPTIONIDBYTITLESQL, [productId, title])
        entity = self.adaptor.mySQLCursor.fetchone()
        if entity is not None:
            optionId = int(entity["option_id"])
        else:
            optionId = 0
        return optionId

    def setCustomOption(self, productId, option):
        title = option.pop("title")
        storeId = option.pop("store_id", 0)
        optionId = self.getCustomOption(productId, title)

        optCols = ["option_id", "product_id", "type", "is_require", "sku", "max_characters", "file_extension", "image_size_x", "image_size_y", "sort_order"]
        optVals = ["%s","%s","%s","%s","%s","%s","%s","%s","%s","%s"]
        values = [
            optionId,
            productId,
            option.pop("type"),
            option.pop("is_require"),
            option.pop("option_sku", None),
            option.pop("max_characters", 0),
            option.pop("file_extension", None),
            option.pop("image_size_x", 0),
            option.pop("image_size_y", 0),
            option.pop("sort_order", 1)
        ]

        if optionId == 0:
            if storeId == 0:
                #optCols.pop("option_id")
                optCols.pop(0)
                optVals.pop(0)
                values.pop(0)

                # Replace custom option.
                self.adaptor.mySQLCursor.execute(
                    self.REPLACECUSTOMOPTIONSQL.format(optCols=",".join(optCols), optVals=",".join(optVals)),
                    values
                )
                optionId = self.adaptor.mySQLCursor.lastrowid
            else:
                # There is no option for store# 0.
                raise Exception("You have to input the option title({}) with store({}) first.".format(title, storeId))
        else:
            # Replace custom option.
            self.adaptor.mySQLCursor.execute(
                self.REPLACECUSTOMOPTIONSQL.format(optCols=",".join(optCols), optVals=",".join(optVals)),
                values
            )

        # Insert custom option title.
        if storeId == 0:
            optionTitle = title
        else:
            optionTitle = option.pop("title_alt")
        self.adaptor.mySQLCursor.execute(
            self.INSERTCUSTOMOPTIONTITLESQL,
            [optionId, storeId, optionTitle, optionTitle]
        )
        optionTitleId = self.adaptor.mySQLCursor.lastrowid

        # insert custom option price.
        if "option_price" in option.keys():
            optionPrice = option.pop("option_price"),
            optionPriceType = option.pop("option_price_type")
            self.adaptor.mySQLCursor.execute(
                self.INSERTCUSTOMOPTIONPRICESQL,
                [
                    optionId,
                    storeId,
                    optionPrice,
                    optionPriceType,
                    optionPrice,
                    optionPriceType
                ]
            )
            optionPriceId = self.adaptor.mySQLCursor.lastrowid

        return optionId

    def getCustomOptionValue(self, optionId, valueTitle):
        self.adaptor.mySQLCursor.execute(self.GETCUSTOMOPTIONTYPEIDBYTITLESQL, [optionId, valueTitle])
        entity = self.adaptor.mySQLCursor.fetchone()
        if entity is not None:
            optionTypeId = int(entity["option_type_id"])
        else:
            optionTypeId = 0
        return optionTypeId

    def setCustomOptionValue(self, optionId, optionValue):
        valueTitle = optionValue.pop("option_value_title")
        storeId = optionValue.pop("store_id", 0)
        optionTypeId = self.getCustomOptionValue(optionId, valueTitle)

        optValCols = ["option_type_id", "option_id", "sku", "sort_order",]
        optValVals = ["%s","%s","%s","%s"]
        values = [
            optionTypeId,
            optionId,
            optionValue.pop("option_value_sku", None),
            optionValue.pop("option_value_sort_order", 1)
        ]

        if optionTypeId == 0:
            if storeId == 0:
                #optValCols.pop("option_type_id")
                optValCols.pop(0)
                optValVals.pop(0)
                values.pop(0)

                # Replace custom option.
                self.adaptor.mySQLCursor.execute(
                    self.REPLACECUSTOMOPTIONTYPEVALUESQL.format(optValCols=",".join(optValCols), optValVals=",".join(optValVals)),
                    values
                )
                optionTypeId = self.adaptor.mySQLCursor.lastrowid
            else:
                # There is no option value for store# 0.
                raise Exception("You have to input the option type title({}) with store({}) first.".format(valueTitle, storeId))
        else:
            # Replace custom option.
            self.adaptor.mySQLCursor.execute(
                self.REPLACECUSTOMOPTIONTYPEVALUESQL.format(optValCols=",".join(optValCols), optValVals=",".join(optValVals)),
                values
            )

        # Insert custom option typle title.
        if storeId == 0:
            optionTypeTitle = valueTitle
        else:
            optionTypeTitle = optionValue.pop("option_value_title_alt")
        self.adaptor.mySQLCursor.execute(
            self.INSERTCUSTOMOPTIONTYPETITLESQL,
            [optionTypeId, storeId, optionTypeTitle, optionTypeTitle]
        )
        optionTypeTitleId = self.adaptor.mySQLCursor.lastrowid

        # insert custom option type price.
        if "option_value_price" in optionValue.keys():
            optionTypePrice = optionValue.pop("option_value_price"),
            optionTypePriceType = optionValue.pop("option_value_price_type")
            self.adaptor.mySQLCursor.execute(
                self.INSERTCUSTOMOPTIONTYPEPRICESQL,
                [
                    optionTypeId,
                    storeId,
                    optionTypePrice,
                    optionTypePriceType,
                    optionTypePrice,
                    optionTypePriceType
                ]
            )
            optionTypePriceId = self.adaptor.mySQLCursor.lastrowid
        return optionTypeId

    def setCustomOptions(self, sku, data):
        productId = self.getProductIdBySku(sku)
        if self.setting["VERSION"] == "EE":
            productId = self.getRowIdByEntityId(productId)
        if data:
            self.adaptor.mySQLCursor.execute(self.DELETECUSTOMOPTIONSQL,[productId])
        for option in data:
            optionId = self.setCustomOption(productId, option)
            if len(option["option_values"]) > 0:
                for optionValue in option["option_values"]:
                    optionTypeId = self.setCustomOptionValue(optionId, optionValue)

        self.adaptor.mySQLCursor.execute(self.UPDATEPRODUCTHASOPTIONSSQL, [productId])

        return productId

    def setImageGallery(self, sku, data):
        productId = self.getProductIdBySku(sku)
        if self.setting["VERSION"] == "EE":
            productId = self.getRowIdByEntityId(productId)

        attributes = dict((k, v) for (k, v) in data.items() if type(v) is not list)
        #self.logger.info(attributes)
        galleryAttributeCode = list(filter(lambda k: (type(data[k]) is list), data.keys()))[0]
        (dataType, galleryAttributeMetadata) = self.getAttributeMetadata(galleryAttributeCode, 'catalog_product')
        #self.logger.info(galleryAttributeCode)
        #self.logger.info(dataType)
        #self.logger.info(galleryAttributeMetadata)
        imageValues = [d['value'] for d in data['media_gallery'] if 'value' in d]
        #self.logger.info(imageValues)
        self.adaptor.mySQLCursor.execute(self.DELETEPRODUCTIMAGEGALLERYEXTSQL,[productId])
        #self.logger.info(self.adaptor.mySQLCursor._last_executed)
        self.adaptor.mySQLCursor.execute(self.DELETEPRODUCTIMAGEGALLERYSQL,[productId])
        #self.logger.info(self.adaptor.mySQLCursor._last_executed)
        for imageValue in imageValues:
            self.adaptor.mySQLCursor.execute(self.DELTEEPRODUCTIMAGEEXTSQL,[imageValue])
            #self.logger.info(self.adaptor.mySQLCursor._last_executed)
        for image in data[galleryAttributeCode]:
            value = image.pop("value")
            storeId = image.pop("store_id", 0)
            label = image.pop("label", "")
            position = image.pop("position", 1)
            mediaType = image.pop("media_type",'image')
            mediaSource = image.pop("media_source",'S3')

            self.adaptor.mySQLCursor.execute(
                self.INSERTPRODUCTIMAGEGALLERYSQL,
                [galleryAttributeMetadata["attribute_id"], value, mediaType]
            )
            #self.logger.info(self.adaptor.mySQLCursor._last_executed)
            valueId = self.adaptor.mySQLCursor.lastrowid

            self.adaptor.mySQLCursor.execute(
                self.INSERTPRODUCTIMAGEGALLERYEXTSQL,
                [valueId, mediaSource, value]
            )
            #self.logger.info(self.adaptor.mySQLCursor._last_executed)

            cols = ["value_id", "store_id", "row_id", "label", "position"] if self.setting["VERSION"] == "EE" \
                else ["value_id", "store_id", "entity_id", "label", "position"]
            vals = ["%s", "%s", "%s", "%s", "%s"]
            params = [valueId, storeId, productId, label, position]
            sql = self.INSERTPRODUCTIMAGEGALLERYVALUESQL.format(cols=",".join(cols), vals=",".join(vals))
            self.adaptor.mySQLCursor.execute(sql, params)

            cols = ["value_id", "row_id"] if self.setting["VERSION"] == "EE" else ["value_id", "entity_id"]
            vals = ["%s", "%s"]
            params = [valueId, productId]
            sql = self.INSERTMEDIAVALUETOENTITYSQL.format(cols=",".join(cols), vals=",".join(vals))
            self.adaptor.mySQLCursor.execute(sql, params)

            attCodes = list(filter(lambda k: (attributes[k] == value), attributes.keys()))
            if len(attCodes) > 0:
                for attCode in attCodes:
                    # assign the attribute.
                    (dataType, attributeMetadata) = self.getAttributeMetadata(attCode, 'catalog_product')
                    cols = ["attribute_id", "store_id", "row_id", "value"] if self.setting["VERSION"] == "EE" \
                        else ["attribute_id", "store_id", "entity_id", "value"]
                    vals = ["%s", "%s", "%s", "%s"]
                    params = [attributeMetadata["attribute_id"], storeId, productId, value, value]
                    sql = self.INSERTPRODUCTIMAGESQL.format(cols=",".join(cols), vals=",".join(vals))
                    self.adaptor.mySQLCursor.execute(sql, params)

        return productId

    def getLinkAttributes(self, code):
        self.adaptor.mySQLCursor.execute(self.SELECTLINKATTSQL, [code])
        row = self.adaptor.mySQLCursor.fetchone()
        if row is not None:
            linkTypeId = int(row["link_type_id"])
            productLinkAttributeId = int(row["product_link_attribute_id"])
        else:
            raise Exception("cannot find link_type_id for {0}.".format(code))
        return (linkTypeId, productLinkAttributeId)

    def setLinks(self, sku, data):
        productId = self.getProductIdBySku(sku)
        if self.setting["VERSION"] == "EE":
            productId = self.getRowIdByEntityId(productId)
        if productId == 0:
            return productId
        for code, links in data.items():
            (linkTypeId, productLinkAttributeId) = self.getLinkAttributes(code)
            if links:
                self.adaptor.mySQLCursor.execute(self.DELETEPRODUCTLINKSQL,[productId,linkTypeId])
            for link in links:
                linkedProductId = self.getProductIdBySku(link["linked_sku"])
                if self.setting["VERSION"] == "EE":
                    linkedProductId = self.getRowIdByEntityId(linkedProductId)
                if linkedProductId == 0:
                    self.logger.warning("sku/link: {0}/{1} failed. Linked product is not found.".format(sku,link))
                    continue
                self.adaptor.mySQLCursor.execute(self.INSERTCATALOGPRODUCTLINKSQL, [productId, linkedProductId, linkTypeId])
                linkId = self.adaptor.mySQLCursor.lastrowid
                self.adaptor.mySQLCursor.execute(self.INSERTCATALOGPRODUCTLINKATTRIBUTEINT, [productLinkAttributeId, linkId, link["position"]])
        return productId

    def _getProductOutOfStockQty(self, productId, websiteId=0, minQty=0):
        outOfStockQty = minQty
        self.adaptor.mySQLCursor.execute(self.GETPRODUCTOUTOFSTOCKQTYSQL, [productId, 1])
        res = self.adaptor.mySQLCursor.fetchone()
        if res is not None and len(res) > 0:
            outOfStockQty = res['min_qty']
        return outOfStockQty

    def setInventory(self, sku, data):
        productId = self.getProductIdBySku(sku)
        metaData = dict((k, list(set(map(lambda d: d[k], data)))) for k in ['store_id'])
        for storeId in metaData["store_id"]:
            websiteId = self._getWebsiteIdByStoreId(storeId)
            qty = sum(int(item["qty"]) for item in list(filter(lambda t: (t["store_id"]==storeId), data)))
            outOfStockQty = self._getProductOutOfStockQty(productId, websiteId=websiteId)
            if qty > int(outOfStockQty):
                isInStock = 1
                stockStatus = 1
            else:
                isInStock = 0
                stockStatus = 0
            self.adaptor.mySQLCursor.execute(self.SETSTOCKSTATUSQL, [productId, websiteId, 1, qty, stockStatus, qty, stockStatus])
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.adaptor.mySQLCursor.execute(self.SETSTOCKITEMSQL, [productId, 1, qty, isInStock, websiteId, qty, isInStock])
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        return productId

    def setProductCategory(self, productId, categoryId, position=0):
        if productId is None or categoryId is None:
            pass
        else:
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            self.adaptor.mySQLCursor.execute(self.SETPRODUCTCATEGORYSQL, [categoryId, productId, position, categoryId, productId, position])
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 1")
            if self.setting["VERSION"] == "EE":
                self.adaptor.mySQLCursor.execute(self.UPDATECATEGORYCHILDRENCOUNTEESQL, [categoryId])
            else:
                self.adaptor.mySQLCursor.execute(self.UPDATECATEGORYCHILDRENCOUNTSQL, [categoryId])

    def getMaxCategoryId(self):
        self.adaptor.mySQLCursor.execute(self.GETMAXCATEGORYIDSQL)
        res = self.adaptor.mySQLCursor.fetchone()
        maxCategoryId = int(res['max_category_id'])
        return maxCategoryId

    def insertCatalogCategoryEntity(self, currentPathIds, attributeSet='Default'):
        entityMetadata = self.getEntityMetaData('catalog_category', attributeSet)
        if entityMetadata == None:
            return 0
        entityId = self.getMaxCategoryId() + 1
        parentId = currentPathIds[-1]
        level = len(currentPathIds)
        pathIds = currentPathIds[:]
        pathIds.append(entityId)
        path = "/".join([str(pathId) for pathId in pathIds])
        childrenCount = 0
        position = 0
        if self.setting["VERSION"] == "EE":
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
            self.adaptor.mySQLCursor.execute(
                self.INSERTCATALOGCATEGORYENTITYEESQL,
                [entityId, entityMetadata['attribute_set_id'], parentId, path, level, childrenCount, position]
            )
            categoryId = self.adaptor.mySQLCursor.lastrowid
            self.adaptor.mySQLCursor.execute("""UPDATE catalog_category_entity SET entity_id = row_id WHERE row_id = %s;""", (categoryId,))
            self.adaptor.mySQLCursor.execute("""INSERT INTO sequence_catalog_category (sequence_value) VALUES (%s);""", (categoryId,))
            self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        else:
            self.adaptor.mySQLCursor.execute(
                self.INSERTCATALOGCATEGORYENTITYSQL,
                [entityMetadata['attribute_set_id'], parentId, path, level, childrenCount, position]
            )
            categoryId = self.adaptor.mySQLCursor.lastrowid
        return categoryId

    def _createCategory(self, currentPathIds, category, storeId):
        categoryId = self.insertCatalogCategoryEntity(currentPathIds)
        urlKey = re.sub('[^0-9a-zA-Z ]+', '', category).replace(" ","-").lower()
        data = {
            'name': category,
            'url_key': urlKey,
            'url_path': urlKey,
            'is_active': '1',
            'is_anchor': '1',
            'include_in_menu': '0',
            'custom_use_parent_settings': '0',
            'custom_apply_to_products': '0',
            'display_mode': 'PRODUCTS',
        }
        self.syncEntityData(categoryId, data, entityTypeCode='catalog_category', storeId=storeId)
        return categoryId

    def _getCategoryId(self, level, parentId, category):
        categoryId = None
        sql = self.GETCATEGORYIDBYATTRIBUTEVALUEANDPATHSQL
        if self.setting["VERSION"] == "EE":
            sql = self.GETCATEGORYIDBYATTRIBUTEVALUEANDPATHEESQL
        self.adaptor.mySQLCursor.execute(sql, [level, parentId, category])
        res = self.adaptor.mySQLCursor.fetchone()
        if res is not None and len(res) > 0:
            categoryId = res['row_id'] if self.setting["VERSION"] == "EE" else res['entity_id']
        return categoryId

    def setCategories(self, sku, data):
        productId = self.getProductIdBySku(sku)

        for row in data:
            storeId = row.pop("store_id", 0)
            delimeter = row.pop("delimeter", "/")
            applyAllLevels = row.pop("apply_all_levels")
            path = row.pop("path")
            position = row.pop("position", 0)
            if path is None or path.strip() == '':
                raise Exception("Path is empty for {0}".format(sku))
            elif productId == 0:
                raise Exception("Product({0}) does not existed in Magento".format(sku))
            else:
                categories = path.split(delimeter)
                try:
                    parentId = 1
                    currentPathIds = [1]
                    for idx in range(0,len(categories)):
                        currentPath = delimeter.join(categories[0:idx+1])
                        category = categories[idx]
                        level = idx + 1
                        categoryId = self._getCategoryId(level, parentId, category)
                        if categoryId is None:
                            categoryId = self._createCategory(currentPathIds, category, storeId)

                        if applyAllLevels == True:
                            if level == 1:
                                parentId = categoryId
                                currentPathIds.append(categoryId)
                                continue
                            else:
                                self.setProductCategory(productId, categoryId, position=position)
                        elif level == len(categories):
                            self.setProductCategory(productId, categoryId, position=position)

                        currentPathIds.append(categoryId)
                        parentId = categoryId
                except Exception:
                    raise
        return productId

    def getVariants(self, productId, attributeIds, adminStoreId=0):
        sql = self.GETCONFIGSIMPLEPRODUCTSSQL.format(
            id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
            store_id=adminStoreId,
            product_id=productId,
            attribute_ids=",".join([str(attributeId) for attributeId in attributeIds])
        )
        self.adaptor.mySQLCursor.execute(sql)
        rows = self.adaptor.mySQLCursor.fetchall()
        variants = []
        if len(rows) > 0:
            metaData = dict((k, list(set(map(lambda d: d[k], rows)))) for k in ['sku'])
            for sku in metaData["sku"]:
                attributes = list(filter(
                        lambda row: row["sku"] == sku,
                        rows
                    )
                )
                variants.append({
                        "sku": sku,
                        "attributes": dict(
                            (
                                attribute["attribute_id"],
                                attribute["value"]
                            ) for attribute in attributes
                        )
                    }
                )
        return variants

    def setVariants(self, sku, data):
        # Validate the child product.
        # data = {
        #     "store_id": "0",
        #     "variant_visibility": True,
        #     "variants": [
        #         {
        #         "variant_sku": "abc",
        #         "attributes": {"att_a": "abc", "att_x": "xyz"}
        #         }
        #     ]
        # }
        parentProductId = self.getProductIdBySku(sku)
        
        attrbuteIds = None
        for product in data.get("variants"):
            attributes = []
            for attributeCode, value in product["attributes"].items():
                (dataType, attributeMetadata) = self.getAttributeMetadata(attributeCode, "catalog_product")
                # Check if the attribute is a "SELECT".
                assert (attributeMetadata['frontend_input'] == 'select'), \
                    "The attribute({attribute_code}) is not a 'SELECT' type.".format(attribute_code=attributeCode)
                # Check if the product has the attribute with the option value.
                optionId = self.getOptionId(attributeMetadata['attribute_id'], value, adminStoreId=data.get("store_id", "0"))
                if optionId is None:
                    options = {0: value}
                    self.setAttributeOptionValues(attributeMetadata['attribute_id'], options, adminStoreId=data.get("store_id", "0"))
                attributes.append(
                    {"attribute_code": attributeCode, "value": value, "attribute_id": attributeMetadata['attribute_id']}
                )
        
            if attrbuteIds is None:
                attributeIds = [attribute["attribute_id"] for attribute in attributes]
            else:
                _attributeIds = [attribute["attribute_id"] for attribute in attributes]
                assert (len(list(set(attributeIds) - set(_attributeIds))) == 0), \
                    "Previous attributes({previous_attribute_ids}) are not matched with attributes({attribute_ids}).".format(
                        previous_attribute_ids=",".join([str(attributeId) for attributeId in attributeIds]),
                        attribute_ids=",".join([str(attributeId) for attributeId in _attributeIds])
                    )

            # Check if there is a product with the same attributes.
            variants = list(filter(lambda product: product["attributes"] == dict(
                    (attribute["attribute_id"], attribute["value"]) for attribute in attributes
                ),
                self.getVariants(parentProductId, attributeIds, adminStoreId=data.get("store_id", "0"))
            ))
            # If there is a product matched with the attribute and value set and the sku is not matched, 
            # raise an exception.
            if len(variants) != 0:
                assert (variants[0]["sku"] == product["variant_sku"]), \
                    "There is already a product({sku}) matched with the attributes({attributes}) of the sku({variant_sku}).".format(
                        sku=variants[0]["sku"],
                        attributes=",".join(["{attribute_code}:{value}".format(attribute_code=attribute["attribute_code"],value=attribute["value"]) for attribute in attributes]),
                        variant_sku=product["variant_sku"]
                    )
        
        # Insert or update the child product.
        for product in data.get("variants"):
            self.logger.info("Insert/Update the item({variant_sku}).".format(variant_sku=product["variant_sku"]))
            for attributeId in attributeIds:
                self.adaptor.mySQLCursor.execute(
                    self.REPLACECATALOGPRODUCTSUPERATTRIBUTESQL.format(
                        product_id=parentProductId,
                        attribute_id=attributeId
                    )
                )

            productId = self.getProductIdBySku(product["variant_sku"])
            if self.setting["VERSION"] == "EE" and productId != 0:
                productId = self.getRowIdByEntityId(productId)
            self.adaptor.mySQLCursor.execute(
                self.REPLACECATALOGPRODUCTRELATIONSQL.format(
                    parent_id=parentProductId, child_id=productId
                )
            )
            self.adaptor.mySQLCursor.execute(
                self.REPLACECATALOGPRODUCTSUPERLINKSQL.format(
                    product_id=productId, parent_id=parentProductId
                )
            )
            self.adaptor.mySQLCursor.execute(
                self.UPDATEPRODUCTVISIBILITYSQL.format(
                    id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                    value=4 if data.get("variant_visibility", False) else 1,
                    product_id=productId
                )
            )
        
        # Visibility for the parent product.
        self.adaptor.mySQLCursor.execute(
            self.UPDATEPRODUCTVISIBILITYSQL.format(
                id="row_id" if self.setting["VERSION"] == "EE" else "entity_id",
                value=4,
                product_id=parentProductId
            )
        )
        return parentProductId

    def syncProductExtData(self, sku, dataType, data):
        try:
            if dataType == "customoption":
                productId = self.setCustomOptions(sku, data)
            elif dataType == "imagegallery":
                productId = self.setImageGallery(sku, data)
            elif dataType == "links":
                productId = self.setLinks(sku, data)
            elif dataType == "inventory":
                productId = self.setInventory(sku, data)
            elif dataType == "categories":
                productId = self.setCategories(sku, data)
            elif dataType == "variants":
                productId = self.setVariants(sku, data)
            else:
                raise Exception("Data Type({0}) is not supported.".format(dataType))

            self.adaptor.commit()
            return productId
        except Exception:
            self.adaptor.rollback()
            raise

    def getTotalProductsCount(self, cutdt, attributeSetName='%', sql=None):
        sql = self.EXPORTPRODUCTSCOUNTSQL if sql is None else sql
        sql = sql.format(
            updated_at=cutdt,
            attribute_set_name=attributeSetName
        )
        self.adaptor.mySQLCursor.execute(sql)
        res = self.adaptor.mySQLCursor.fetchone()
        return res['total']

    def getImages(self, cutdt, offset=None, limit=None, sql=None):
        sql = (
            self.EXPORTMEDIAIMAGESEESQL if self.setting["VERSION"] == "EE" else self.EXPORTMEDIAIMAGESSQL
        ) if sql is None else sql
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        result = []
        imageTypes = ["image", "small_image", "thumbnail"]
        for imageType in imageTypes:
            sql = sql.format(
                base_url=self.setting["MEDIABASEURL"],
                image_type=imageType,
                attribute_code=imageType,
                updated_at=cutdt
            )
            self.adaptor.mySQLCursor.execute(sql)
            res = self.adaptor.mySQLCursor.fetchall()
            result.extend(res)
        return result

    def getGallery(self, cutdt, offset=None, limit=None, sql=None):
        sql = (
            self.EXPORTMEDIAGALLERYEESQL if self.setting["VERSION"] == "EE" else self.EXPORTMEDIAGALLERYSQL
        ) if sql is None else sql
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        sql = sql.format(
            base_url=self.setting["MEDIABASEURL"],
            updated_at=cutdt
        )
        self.adaptor.mySQLCursor.execute(sql)
        res = self.adaptor.mySQLCursor.fetchall()
        return res

    def getOrders(self, cutdt):
        sql = self.GETORDERSSQL.format(updated_at=cutdt)
        self.adaptor.mySQLCursor.execute(sql)
        rawOrders = self.adaptor.mySQLCursor.fetchall()
        for order in rawOrders:
            sql = self.GETORDERITEMSSQL.format(order_id=order['id'])
            self.adaptor.mySQLCursor.execute(sql)
            order['items'] = self.adaptor.mySQLCursor.fetchall()
        return rawOrders