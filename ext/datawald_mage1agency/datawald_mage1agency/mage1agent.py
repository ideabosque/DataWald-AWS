from mage1agency import Mage1Agency


class Mage1Agent(Mage1Agency):
    def __init__(self, setting=None, logger=None, feApp='mage1', boApp='mage1', dataWald=None, feConn=None, boConn=None):
        Mage1Agency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)
        self.mage1.EXPORTPRODUCTSSQL = """
            SELECT a.*, b.attribute_set_name
            FROM catalog_product_entity a
            INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
            WHERE updated_at >= %s AND b.attribute_set_name LIKE %s AND a.sku LIKE 'mcc-%%'
        """

        self.mage1.EXPORTPRODUCTSCOUNTSQL = """
            SELECT count(*) AS total
            FROM catalog_product_entity a
            INNER JOIN eav_attribute_set b ON a.attribute_set_id = b.attribute_set_id
            WHERE updated_at >= %s AND b.attribute_set_name LIKE %s AND a.sku LIKE 'mcc-%%'
        """

        self.mage1.EXPORTSTOCKSQL = """
            SELECT a.item_id, b.sku, 'admin' as website_code, a.qty, a.is_in_stock
            FROM cataloginventory_stock_item a
            INNER JOIN catalog_product_entity b on a.product_id = b.entity_id
            WHERE b.updated_at >= %s AND b.sku LIKE 'mcc-%%'
        """

        self.mage1.EXPORTSTOCKCOUNTSQL = """
            SELECT count(*) AS total
            FROM cataloginventory_stock_item a
            INNER JOIN catalog_product_entity b on a.product_id = b.entity_id
            WHERE b.updated_at >= %s AND b.sku LIKE 'mcc-%%'
        """

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    def boProductsExtDataExtFt(self, dataType, productsExtData, rawProductsExtData):
        pass

    def getCustomerGroupPrice(self,sku,customerGroupCode):
        sql = """
            SELECT value
            FROM catalog_product_entity_group_price a
            INNER JOIN catalog_product_entity b ON a.entity_id = b.entity_id
            INNER JOIN customer_group c ON a.customer_group_id = c.customer_group_id
            WHERE b.sku = %s AND c.customer_group_code = %s
        """
        self.mage1.adaptor.mySQLCursor.execute(sql,[sku,customerGroupCode])
        res = self.mage1.adaptor.mySQLCursor.fetchone()
        customerGroupPrice = '0'
        if res is not None:
            customerGroupPrice = str(res['value'])
        return customerGroupPrice

    def boProductsExtFt(self, products, rawProducts):
        for product in products:
            product['data']['cost'] = distributorPrice = product['raw_data']['price'] if self.getCustomerGroupPrice(product['sku'],'Distributor') == '0' else self.getCustomerGroupPrice(product['sku'],'Distributor')
            product['data']['price'] = sellPrice = product['raw_data']['price'] if self.getCustomerGroupPrice(product['sku'],'Wholesale T1') == '0' else self.getCustomerGroupPrice(product['sku'],'Wholesale T1')
        #pass
        # self.logger.info("Products count before removal: {0}".format(len(products)))
        # idx = 0
        # needRemoveIdx = []
        # for product in products:
        #     self.logger.info(product['sku'])
        #     if not product['sku'].lower().startswith('mcc-'):
        #         needRemoveIdx.append(idx)
        #         self.logger.info("I will be deleted: {0}".format(product['sku']))
        #     idx = idx + 1
        # for i in needRemoveIdx:
        #     del products[i]
        # self.logger.info("Products count after removal: {0}".format(len(products)))
