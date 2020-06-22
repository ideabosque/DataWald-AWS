import MySQLdb
import suds
from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor
from suds.plugin import MessagePlugin
import traceback

class MagentoApiMessagePlugin(MessagePlugin):
    def marshalled(self, context):
        body = context.envelope.getChild("Body")
        call = context.envelope.childAtPath("Body/call")
        if call:
            resourcePath = call.getChild("resourcePath")
            if resourcePath is not None and (str(resourcePath.getText()) == 'sales_order_shipment.create' or str(resourcePath.getText()) == 'sales_order_invoice.create'):
                args = call.getChild("args")
                if args:
                    item = args.getChild("item")
                    if item:
                        item.set("xsi:type","http://xml.apache.org/xml-soap:Map")
        return context


class Adaptor(object):
    """Adaptor contain MySQL cursor object.
    """

    def __init__(self, mySQLConn=None):
        self._mySQLConn = mySQLConn
        self._mySQLCursor = self._mySQLConn.cursor(MySQLdb.cursors.DictCursor)

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

class ApiAdaptor(object):
    """Adaptor contain API connection
    """

    def __init__(self,apiConn=None,apiSessionId=None):
        self._apiConn = apiConn
        self._apiSessionId = apiSessionId

    @property
    def apiConn(self):
        return self._apiConn

    @property
    def apiSessionId(self):
        return self._apiSessionId

    def disconnect(self):
        if self._apiSessionId is not None and self._apiConn is not None:
            try:
                result = self._apiConn.service.endSession(self._apiSessionId)
                self.logger.info("Logout Magento 1 API: sessionId = {0}".format(self._apiSessionId))
            except Exception as e:
                self.logger.exception("Failed to logout Magento 1 API with error: {0}".format(e))
                raise


class Mage1Connector(object):
    """Magento 1 connection with functions.
    """

    GETPRODUCTIDBYSKUSQL = """SELECT distinct entity_id FROM catalog_product_entity WHERE sku = %s;"""

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

    ISENTITYEXITSQL = """SELECT count(*) as count FROM {0}_entity WHERE entity_id = %s;"""

    ISATTRIBUTEVALUEEXITSQL = """
        SELECT count(*) as count
        FROM {0}_entity_{1}
        WHERE attribute_id = %s
        AND store_id = %s
        AND {2} = %s;"""

    REPLACEATTRIBUTEVALUESQL = """REPLACE INTO {0}_entity_{1} ({2}) values ({3});"""

    UPDATEENTITYUPDATEDATSQL = """UPDATE {0}_entity SET updated_at = UTC_TIMESTAMP() WHERE entity_id = %s;"""

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
        WHERE {0} = %s;"""

    INSERTEAVATTRIBUTEOPTIONSQL = """INSERT INTO eav_attribute_option (attribute_id) VALUES (%s);"""

    OPTIONVALUEEXISTSQL = """
        SELECT COUNT(*) as cnt FROM eav_attribute_option_value
        WHERE option_id = %s
        AND store_id = %s;"""

    INSERTOPTIONVALUESQL = """INSERT INTO eav_attribute_option_value (option_id, store_id, value) VALUES (%s, %s, %s);"""

    UPDATEOPTIONVALUESQL = """UPDATE eav_attribute_option_value SET value = %s WHERE option_id = %s AND store_id = %s;"""

    GETATTRIBUTESBYENTITYTYPEANDATTRIBUTESETSQL = """
        SELECT
        a.entity_type_id, b.entity_type_code,
        d.attribute_set_id, d.attribute_set_name,
        a.attribute_id, a.attribute_code,
        a.backend_type, a.frontend_input, a.frontend_label, a.is_required, a.is_user_defined
        FROM eav_attribute a
        INNER JOIN eav_entity_attribute c on (a.attribute_id = c.attribute_id and a.entity_type_id = c.entity_type_id)
        INNER JOIN eav_attribute_set d on (c.attribute_set_id = d.attribute_set_id)
        INNER JOIN eav_entity_type b on (a.entity_type_id = b.entity_type_id)
        WHERE b.entity_type_code = %s and d.attribute_set_name = %s
    """

    GETATTRIBUTESBYENTITYTYPESQL = """
        SELECT
        a.entity_type_id, b.entity_type_code,
        a.attribute_id, a.attribute_code,
        a.backend_type, a.frontend_input, a.frontend_label, a.is_required, a.is_user_defined
        FROM eav_attribute a
        INNER JOIN eav_entity_type b on (a.entity_type_id = b.entity_type_id)
        WHERE b.entity_type_code = %s
    """

    GETATTRIBUTEVALUESQL = "SELECT value FROM {0}_entity_{1} WHERE attribute_id = %s AND entity_id = %s"
    GETATTRIBUTEOPTIONVALUESQL = """
        SELECT
        t3.value
        FROM
        eav_attribute_option t2,
        eav_attribute_option_value t3
        WHERE
        t2.option_id = t3.option_id
        AND t2.attribute_id = %s
        AND t3.option_id = %s
        AND t3.store_id = %s
    """

    EXPORTPRODUCTSSQL = """
        SELECT a.*, b.attribute_set_name
        FROM catalog_product_entity a
        INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
        WHERE updated_at >= %s AND b.attribute_set_name LIKE %s
    """

    EXPORTPRODUCTSCOUNTSQL = """
        SELECT count(*) AS total
        FROM catalog_product_entity a
        INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
        WHERE updated_at >= %s AND b.attribute_set_name LIKE %s
    """

    EXPORTSTOCKSQL = """
        SELECT a.item_id, b.sku, 'admin' as website_code, a.qty, a.is_in_stock
        FROM cataloginventory_stock_item a
        INNER JOIN catalog_product_entity b on a.product_id = b.entity_id
        WHERE b.updated_at >= %s
    """

    EXPORTSTOCKCOUNTSQL = """
        SELECT count(*) AS total
        FROM cataloginventory_stock_item a
        INNER JOIN catalog_product_entity b on a.product_id = b.entity_id
        WHERE b.updated_at >= %s
    """

    ISCANINVOICESQL = """
        SELECT entity_id, increment_id, base_grand_total, base_total_invoiced
        FROM sales_flat_order
        WHERE increment_id = %s
    """

    GETINVOICEINCIDSQL = """
        SELECT t1.increment_id
        FROM sales_flat_order t0, sales_flat_invoice t1
        WHERE t0.entity_id = t1.order_id
        AND t0.increment_id = %s
    """

    GETORDERLINEITEMBYSKUSQL = """
        SELECT a.item_id
        FROM sales_flat_order_item a, sales_flat_order b
        WHERE a.parent_item_id is null AND
            a.order_id = b.entity_id AND
            a.sku = %s AND
            b.increment_id = %s
    """

    EXPORTMEDIAIMAGESSQL = """
        SELECT
        t0.sku,
        CONCAT('{0}', t1.value) as 'value',
        %s as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_varchar t1,
        eav_attribute t2
        WHERE t0.entity_id = t1.entity_id
        AND t1.attribute_id = t2.attribute_id
        AND t2.attribute_code = %s
        AND t0.updated_at >= %s
    """

    EXPORTMEDIAGALLERYSQL = """
        SELECT
        t0.sku,
        CONCAT('{0}', t1.value) as 'value',
        t2.store_id,
        t2.position,
        t2.label,
        'mage1' as 'media_source',
        'media_gallery' as 'type'
        FROM
        catalog_product_entity t0,
        catalog_product_entity_media_gallery t1,
        catalog_product_entity_media_gallery_value t2
        WHERE t0.entity_id = t1.entity_id
        AND t1.value_id = t2.value_id
        AND t0.updated_at >= %s
    """

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger
        self._adaptor = None
        self._apiAdaptor = None

    def connect(self,type=None):
        """Initiate the connect with MySQL server or API connection.
        """
        if type == 'API':
            suds.bindings.binding.envns = ('SOAP-ENV', 'http://schemas.xmlsoap.org/soap/envelope/')
            imp = Import('http://schemas.xmlsoap.org/soap/encoding/')
            imp.filter.add('urn:Magento')
            doctor = ImportDoctor(imp)
            if 'MAGE1APIHTTPUSERNAME' in self.setting:
                mageApiConn = Client(
                                    self.setting['MAGE1WSDL'],
                                    doctor=doctor,
                                    plugins=[MagentoApiMessagePlugin()],
                                    username = self.setting['MAGE1APIHTTPUSERNAME'],
                                    password = self.setting['MAGE1APIHTTPPASSWORD'])
            else :
                mageApiConn = Client(
                                    self.setting['MAGE1WSDL'],
                                    doctor=doctor,
                                    plugins=[MagentoApiMessagePlugin()])
            apiSessionId = mageApiConn.service.login(self.setting['MAGE1APIUSER'], self.setting['MAGE1APIKEY'])
            return ApiAdaptor(mageApiConn,apiSessionId)
        elif type == "CURSOR":
            mySQLConn = MySQLdb.connect(user=self.setting['MAGE1DBUSERNAME'],
                                        passwd=self.setting['MAGE1DBPASSWORD'],
                                        db=self.setting['MAGE1DB'],
                                        host=self.setting['MAGE1DBSERVER'],
                                        port=self.setting['MAGE1DBPORT'],
                                        charset="utf8",
                                        use_unicode=False)
            log = "Open Mage1 DB connection"
            self.logger.info(log)
            return Adaptor(mySQLConn)
        else:
            return None

    @property
    def adaptor(self):
        self._adaptor = self.connect(type="CURSOR") if self._adaptor is None else self._adaptor
        return self._adaptor

    @property
    def apiAdaptor(self):
        self._apiAdaptor = self.connect(type="API") if self._apiAdaptor is None else self._apiAdaptor
        return self._apiAdaptor

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
        if attributeMetadata is None or len(attributeMetadata) < 4 :
            log = "Entity Type/Attribute Code: {0}/{1} does not exist".format(entityTypeCode, attributeCode)
            raise Exception(log)
        if attributeCode == 'url_key' and self.setting['VERSION'] == "EE":
            dataType = attributeCode
        else:
            dataType = attributeMetadata['backend_type']
        return (dataType, attributeMetadata)

    def isEntityExit(self, entityTypeCode, entityId):
        sql = self.ISENTITYEXITSQL.format(entityTypeCode)
        self.adaptor.mySQLCursor.execute(sql, [entityId])
        exist = self.adaptor.mySQLCursor.fetchone()
        return exist['count']

    def isAttributeValueExit(self, entityTypeCode, dataType, attributeId, storeId, entityId):
        key = 'row_id' if self.setting['VERSION'] == "EE" else 'entity_id'
        sql = self.ISATTRIBUTEVALUEEXITSQL.format(entityTypeCode, dataType, key)
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
        sql = self.REPLACEATTRIBUTEVALUESQL.format(entityTypeCode, dataType, cols, vls)
        self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        self.adaptor.mySQLCursor.execute(sql, param)
        self.adaptor.mySQLCursor.execute("SET FOREIGN_KEY_CHECKS = 1")

    def updateEntityUpdatedAt(self, entityTypeCode, entityId):
        sql = self.UPDATEENTITYUPDATEDATSQL.format(entityTypeCode)
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
        sql = self.UPDATECATALOGPRODUCTSQL.format(key)
        self.adaptor.mySQLCursor.execute(sql, [entityMetadata['attribute_set_id'], typeId, productId])

    def syncProductData(self, attributeSet, entityId, data, storeId=0, adminStoreId=0):
        doNotUpdateOptionAttributes = ['status','visibility','tax_class_id']
        for attributeCode, value in data.items():
            (dataType, attributeMetadata) = self.getAttributeMetadata(attributeCode, 'catalog_product')
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
                exist = self.isAttributeValueExit('catalog_product', dataType, attributeMetadata['attribute_id'], adminStoreId, entityId)
                storeId = adminStoreId if exist == 0 else storeId
                self.replaceAttributeValue("catalog_product", dataType, entityId, attributeMetadata['attribute_id'], value, storeId=storeId)

    def syncProduct(self, sku, attributeSet, data, typeId, storeId):
        try:
            productId = self.getProductIdBySku(sku)
            if productId == 0:
                productId = self.insertCatalogProductEntity(sku, attributeSet, typeId)
            else:
                self.updateCatalogProductEntity(productId, attributeSet, typeId)
            self.syncProductData(attributeSet, productId, data, storeId=storeId)
            self.adaptor.commit()
            return productId
        except Exception as e:
            self.adaptor.rollback()
            raise

    ## TO DO: Implement the logic
    def _getOrderCustomerEmail(self,order):
        #return self.setting['MAGE1ORDERCUSTOMEREMAIL']
        return order['addresses']["billto"]['email']

    ## TO DO: Implement the logic
    def _getOrderShippingMethod(self,order):
        return self.setting['MAGE1ORDERSHIPPINGMETHOD']

    ## TO DO: Implement the logic
    def _getOrderPaymentMethod(self,order):
        return self.setting['MAGE1ORDERPAYMENTMETHOD']

    def insertOrder(self, order):
        orderNum = None
        cartId = self.apiAdaptor.apiConn.service.shoppingCartCreate(self.apiAdaptor.apiSessionId,self.setting['MAGE1ORDERSTOREID'])
        self.logger.info("Shopping cart created successfully, cartId: {0}".format(cartId))
        # get line items
        for item in order['items']:
            productFilter = {
                'complex_filter': {
                    'item': {
                        'key': 'sku',
                        'value': {
                            'key': 'in',
                            'value': item['sku']
                        }
                    }
                }
            }
            productList = self.apiAdaptor.apiConn.service.catalogProductList(self.apiAdaptor.apiSessionId, productFilter)
            product = productList[0]
            cartProduct = self.apiAdaptor.apiConn.factory.create('shoppingCartProductEntity')
            cartProduct['qty'] = item['qty']
            cartProduct['product_id'] = product['product_id']
            cartProduct['sku'] = product['sku']
            # cartProduct['options']
            # cartProduct['bundle_option']
            # cartProduct['bundle_option_qty']
            # cartProduct['links']
            self.apiAdaptor.apiConn.service.shoppingCartProductAdd(self.apiAdaptor.apiSessionId, cartId, [cartProduct])
            # print "cartProduct = {}".format(cartProduct)

        # Customer Info
        customerFilter = {
            'complex_filter': {
                'item': {
                    'key': 'email',
                    'value': {
                        'key': 'in',
                        'value': self._getOrderCustomerEmail(order)
                    }
                }
            }
        }
        customerList = self.apiAdaptor.apiConn.service.customerCustomerList(self.apiAdaptor.apiSessionId, customerFilter)
        customer = customerList[0]

        cartCustomer = self.apiAdaptor.apiConn.factory.create('shoppingCartCustomerEntity')
        cartCustomer['mode'] = 'customer'
        cartCustomer['customer_id'] = customer['customer_id']
        cartCustomer['email'] = customer['email']
        cartCustomer['firstname'] = customer['firstname']
        cartCustomer['lastname'] = customer['lastname']
        cartCustomer['website_id'] = customer['website_id']
        cartCustomer['store_id'] = customer['store_id']
        cartCustomer['group_id'] = customer['group_id']
        # cartCustomer['password']
        # cartCustomer['confirmation']
        self.apiAdaptor.apiConn.service.shoppingCartCustomerSet(self.apiAdaptor.apiSessionId, cartId, cartCustomer)
        # print "cartCustomer = {}".format(cartCustomer)

        # Bill-To Address
        billingAddress = self.apiAdaptor.apiConn.factory.create('shoppingCartCustomerAddressEntity')
        billingAddress['mode'] = 'billing'
        billingAddress['firstname'] = order['addresses']["billto"]['firstname']
        billingAddress['lastname'] = order['addresses']["billto"]['lastname']
        billingAddress['street'] = order['addresses']["billto"]['address'] if order['addresses']["shipto"]['address'].strip() != "" else "Street"
        billingAddress['city'] = order['addresses']["billto"]['city']
        billingAddress['region'] = order['addresses']["billto"]['region']
        # billingAddress['region_id'] = 12
        billingAddress['postcode'] = order['addresses']["billto"]['postcode']
        billingAddress['country_id'] = order['addresses']["billto"]['country']
        billingAddress['telephone'] = order['addresses']["billto"]['telephone']
        # billingAddress['address_id']
        # billingAddress['company']
        # billingAddress['fax']
        # billingAddress['is_default_billing']
        # billingAddress['is_default_shipping']
        # print "billingAddress = {}".format(billingAddress)

        # Ship-To Address
        shippingAddress = self.apiAdaptor.apiConn.factory.create('shoppingCartCustomerAddressEntity')
        shippingAddress['mode'] = 'shipping'
        shippingAddress['firstname'] = order['addresses']["shipto"]['firstname']
        shippingAddress['lastname'] = order['addresses']["shipto"]['lastname']
        shippingAddress['street'] = order['addresses']["shipto"]['address'] if order['addresses']["shipto"]['address'].strip() != "" else "Street"
        shippingAddress['city'] = order['addresses']["shipto"]['city']
        shippingAddress['region'] = order['addresses']["shipto"]['region']
        # shippingAddress['region_id'] = 12
        shippingAddress['postcode'] = order['addresses']["shipto"]['postcode']
        shippingAddress['country_id'] = order['addresses']["shipto"]['country']
        shippingAddress['telephone'] = order['addresses']["shipto"]['telephone']

        self.apiAdaptor.apiConn.service.shoppingCartCustomerAddresses(self.apiAdaptor.apiSessionId, cartId, [billingAddress, shippingAddress])
        # print "shippingAddress = {}".format(shippingAddress)

        # Shipping Method
        # TODO
        # shippingMethodCode = order['shipment_method']
        shippingMethodCode = self._getOrderShippingMethod(order)
        try:
            self.apiAdaptor.apiConn.service.shoppingCartShippingMethod(self.apiAdaptor.apiSessionId, cartId, shippingMethodCode)
        except Exception as e:
            self.logger.info("Failed to set shipping method '{0}' for order {1} error: {2}".format(shippingMethodCode, order['id'], e))
            # raise

            # TODO
            shippingMethodList = self.apiAdaptor.apiConn.service.shoppingCartShippingList(self.apiAdaptor.apiSessionId, cartId)
            # print "shippingMethodList = {}".format(shippingMethodList)
            shippingMethodCode = shippingMethodList[0]['code']
            self.apiAdaptor.apiConn.service.shoppingCartShippingMethod(self.apiAdaptor.apiSessionId, cartId, shippingMethodCode)

        # Payment Method
        # TODO
        # paymentMethodCode = order['payment_method']
        paymentMethodCode = self._getOrderPaymentMethod(order)
        paymentMethod = self.apiAdaptor.apiConn.factory.create('shoppingCartPaymentMethodEntity')
        paymentMethod['method'] = paymentMethodCode
        # paymentMethod['po_number']
        # paymentMethod['cc_cid']
        # paymentMethod['cc_owner']
        # paymentMethod['cc_number']
        # paymentMethod['cc_type']
        # paymentMethod['cc_exp_year']
        # paymentMethod['cc_exp_month']
        try:
            self.apiAdaptor.apiConn.service.shoppingCartPaymentMethod(self.apiAdaptor.apiSessionId, cartId, paymentMethod)
        except Exception as e:
            self.logger.info("Failed to set payment method {0} for order {1} error: {2}".format(paymentMethodCode, order['id'], e))
            # raise

            # TODO
            paymentMethodList = self.apiAdaptor.apiConn.service.shoppingCartPaymentList(self.apiAdaptor.apiSessionId, cartId)
            # print "paymentMethodList = {}".format(paymentMethodList)
            paymentMethod['method'] = paymentMethodList[0]['code']
            self.apiAdaptor.apiConn.service.shoppingCartPaymentMethod(self.apiAdaptor.apiSessionId, cartId, paymentMethod)

        orderNum = self.apiAdaptor.apiConn.service.shoppingCartOrder(self.apiAdaptor.apiSessionId, cartId)
        self.logger.info("Order created successfully, order Number: {0}".format(orderNum))
        return orderNum

    def insertOrders(self, orders):
        for order in orders:
            try:
                order["bo_order_id"] = self.insertOrder(order)
                order["tx_status"] = 'S'
            except Exception as e:
                log = traceback.format_exc()
                order["bo_order_id"] = "####"
                order["tx_status"] = 'F'
                order["tx_note"] = log
                self.logger.exception('Failed to create order: {0} with error: {1}'.format(order, e))
        return orders

    def getAttributesByEntityTypeAndAttributeSet(self,entityTypeCode,attributeSetName):
        self.adaptor.mySQLCursor.execute(self.GETATTRIBUTESBYENTITYTYPEANDATTRIBUTESETSQL,[entityTypeCode, attributeSetName])
        res = self.adaptor.mySQLCursor.fetchall()
        attributes = {}
        for row in res:
            attributes[row['attribute_code']] = row
        return attributes

    def getAttributesByEntityType(self,entityTypeCode):
        self.adaptor.mySQLCursor.execute(self.GETATTRIBUTESBYENTITYTYPESQL,[entityTypeCode])
        res = self.adaptor.mySQLCursor.fetchall()
        attributes = {}
        for row in res:
            attributes[row['attribute_code']] = row
        return attributes

    def getAttributeOptionValue(self,attributeId, entityId, storeId=0):
        self.adaptor.mySQLCursor.execute(self.GETATTRIBUTEOPTIONVALUESQL,[attributeId, entityId, storeId])
        item = self.adaptor.mySQLCursor.fetchone()
        if item is not None:
            value = item['value']
            return value
        return None

    def getAttributeValue(self,entityTypeCode,attributeCode,entityId,storeId=0):
        attributeMetadata = self.getAttributeMetadata(attributeCode,entityTypeCode)
        (dataType, attributeMetadata) = self.getAttributeMetadata(attributeCode, entityTypeCode)
        if attributeMetadata is None:
            self.logger.info("Attribute metadata is not found for {0}".format(attributeCode))
            return None
        attributeId = attributeMetadata['attribute_id']
        if entityId and attributeMetadata['backend_type'] in ['varchar','text','int','decimal','datetime'] and entityId != '':
            if entityTypeCode in ['catalog_category','catalog_product'] :
                getAttributeValueSQL = self.GETATTRIBUTEVALUESQL + " AND store_id = {0}".format(storeId)
            else :
                getAttributeValueSQL = self.GETATTRIBUTEVALUESQL
            getAttributeValueSQL = getAttributeValueSQL.format(entityTypeCode, attributeMetadata['backend_type'])
            self.adaptor.mySQLCursor.execute(getAttributeValueSQL,[attributeId,entityId])
            res = self.adaptor.mySQLCursor.fetchone()
            value = None
            if res is not None:
                value = res['value']
            if value is not None and attributeMetadata['frontend_input'] == 'select' and attributeMetadata['backend_type'] == 'int':
                optionValue = self.getAttributeOptionValue(attributeId, value)
                if optionValue is not None :
                    value = optionValue
            if value is not None and attributeMetadata['frontend_input'] == 'multiselect' :
                valueIdsList = [v.strip() for v in value.split(',')]
                values = []
                for v in valueIdsList :
                    vStr =  self.getAttributeOptionValue(attributeId, v)
                    if vStr is not None:
                        values.append(vStr)
                value = ",".join(values)
            return value
        else :
            return None

    def getProducts(self,attributeSetName,cutDt,exportAttributes=[],limit=None,offset=None):
        if attributeSetName != '':
            attributes = self.getAttributesByEntityTypeAndAttributeSet('catalog_product',attributeSetName)
        else :
            attributes = self.getAttributesByEntityType('catalog_product')
            attributeSetName = "%"
        sql = self.EXPORTPRODUCTSSQL
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        self.adaptor.mySQLCursor.execute(sql,[cutDt,attributeSetName])
        products = self.adaptor.mySQLCursor.fetchall()
        allProducts = []
        cnt = 1
        for product in products :
            for attributeCode in attributes :
                if len(exportAttributes) > 0 and attributeCode not in exportAttributes:
                    continue
                if attributeCode in product :
                    if product[attributeCode] is not None :
                        product[attributeCode] = str(product[attributeCode])
                    continue
                attributeValue = self.getAttributeValue('catalog_product',attributeCode,product['entity_id'])
                if attributeValue is not None:
                    attributeValue = str(attributeValue)
                product[attributeCode] = attributeValue
            for k, v in product.items():
                product[k] = str(v)
            allProducts.append(product)
            cnt = cnt + 1
        return allProducts

    def getInventory(self, cutdt,offset=None,limit=None):
        sql = self.EXPORTSTOCKSQL
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        self.adaptor.mySQLCursor.execute(sql,[cutdt])
        res = self.adaptor.mySQLCursor.fetchall()
        return res

    def getTotalProductsCount(self,cutdt,attributeSetName='%'):
        self.adaptor.mySQLCursor.execute(self.EXPORTPRODUCTSCOUNTSQL,[cutdt,attributeSetName])
        res = self.adaptor.mySQLCursor.fetchone()
        return res['total']

    def getTotalInventoryCount(self,cutdt):
        self.adaptor.mySQLCursor.execute(self.EXPORTSTOCKCOUNTSQL,[cutdt])
        res = self.adaptor.mySQLCursor.fetchone()
        return res['total']

    def isCanInvoice(self,mOrderIncId):
        self.adaptor.mySQLCursor.execute(self.ISCANINVOICESQL,[mOrderIncId])
        res = self.adaptor.mySQLCursor.fetchone()
        flag = False
        if res is not None:
            baseTotalInvoiced = res['base_total_invoiced']
            baseGrandTotal = res['base_grand_total']
            if baseTotalInvoiced < baseGrandTotal:
                flag = True
        return flag

    def getInvoiceIncIdByOrderIncId(self, orderIncId):
        self.adaptor.mySQLCursor.execute(self.GETINVOICEINCIDSQL, [orderIncId])
        res = self.adaptor.mySQLCursor.fetchall()
        if len(res) > 0:
            invoiceIncIds = [entity['increment_id'] for entity in res]
            return ",".join(invoiceIncIds)
        else:
            return None

    def getOrderLineItemBySku(self,mOrderIncId,sku):
        self.adaptor.mySQLCursor.execute(self.GETORDERLINEITEMBYSKUSQL,[sku,mOrderIncId])
        row = self.adaptor.mySQLCursor.fetchone()
        if row is not None:
            return int(row['item_id'])
        else :
            return None

    def insertInvoice(self, invoice):
        orderIncId = invoice['fe_order_id']
        if self.isCanInvoice(orderIncId):
            itemsQty = {}
            for line in invoice['data']['items']:
                sku = line['sku']
                qty = line['qty']
                orderItemId = self.getOrderLineItemBySku(orderIncId,sku)
                if orderItemId is None:
                    continue
                if orderItemId in itemsQty:
                    itemsQty[orderItemId] = itemsQty[orderItemId] + qty
                else:
                    itemsQty[orderItemId] = qty
            items = []
            for k, v in itemsQty.items():
                items.append({'order_item_id':str(k),'qty':str(float(v))})
            params = self.apiAdaptor.apiConn.factory.create('{http://schemas.xmlsoap.org/soap/encoding/}Array')
            params = {'item':items}
            comment = ''
            invoiceNumber = self.apiAdaptor.apiConn.service.salesOrderInvoiceCreate(
                self.apiAdaptor.apiSessionId,
                orderIncId,
                params,
                comment,
                self.setting['MAGE1EMAILINVOICE'],
                self.setting['MAGE1INVOICEEMAILCOMMENT']
            )
            self.logger.info("Invoice created successfully, invoice number: {0}".format(invoiceNumber))
            return invoiceNumber
        else :
            invoiceNumber = self.getInvoiceIncIdByOrderIncId(orderIncId)
            if invoiceNumber is not None:
                return invoiceNumber
            else:
                log = "Order is not able to invoiced, order number: {0}".format(orderIncId)
                self.logger.exception(log)
                raise Exception(log)

    def getImages(self, cutdt, offset=None, limit=None):
        sql = self.EXPORTMEDIAIMAGESSQL.format(self.setting["MEDIABASEURL"])
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        result = []
        imageTypes = ["image", "small_image", "thumbnail"]
        for imageType in imageTypes:
            self.adaptor.mySQLCursor.execute(sql, [imageType, imageType, cutdt])
            res = self.adaptor.mySQLCursor.fetchall()
            result.extend(res)
        return result

    def getGallery(self, cutdt, offset=None, limit=None):
        sql = self.EXPORTMEDIAGALLERYSQL.format(self.setting["MEDIABASEURL"])
        if offset is not None and limit is not None:
            sql = sql + " LIMIT {0} OFFSET {1}".format(limit,offset)
        elif limit is not None:
            sql = sql + " LIMIT {0}".format(limit)
        self.adaptor.mySQLCursor.execute(sql, [cutdt])
        res = self.adaptor.mySQLCursor.fetchall()
        return res
