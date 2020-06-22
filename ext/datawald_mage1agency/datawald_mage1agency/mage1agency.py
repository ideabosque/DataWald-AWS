from datawald_frontend import FrontEnd
from datawald_backoffice import BackOffice
from datetime import datetime, timedelta
import copy
import traceback

class Mage1Agency(FrontEnd, BackOffice):
    def __init__(self, setting=None, logger=None, feApp=None, boApp=None, dataWald=None, feConn=None, boConn=None):
        self.setting = setting
        self.logger = logger
        if feConn is not None:
            self.mage1 = feConn
            FrontEnd.__init__(self, logger=logger, feApp=feApp, dataWald=dataWald)
        elif boConn is not None:
            self.mage1 = boConn
            BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

        self._orderQueryMap = {
            'fields' : {
                'id' : 'sales_order.entity_id',
                'm_order_inc_id' : 'sales_order.increment_id',
                'm_order_date' : 'sales_order.created_at',
                'm_order_update_date' : 'sales_order.updated_at',
                'm_order_status' : 'sales_order.status',
                'm_customer_group' : 'customer_group.customer_group_code',
                'm_store_id' : 'sales_order.store_id',
                'm_customer_id' : 'sales_order.customer_id',
                'shipment_carrier' : "''",
                'shipment_method' : 'IFNULL(sales_order.shipping_method,"")',
                'billto_firstname' : 'IFNULL(bill_to.firstname,"")',
                'billto_lastname' : 'IFNULL(bill_to.lastname,"")',
                'billto_email' : 'IFNULL(bill_to.email,"")',
                'billto_companyname' : 'IFNULL(bill_to.company,"")',
                'billto_address' : 'IFNULL(bill_to.street,"")',
                'billto_city' : 'IFNULL(bill_to.city,"")',
                'billto_region' : 'IFNULL(bill_to_region.code,"")',
                'billto_country' : 'IFNULL(bill_to.country_id,"")',
                'billto_postcode' : 'IFNULL(bill_to.postcode,"")',
                'billto_telephone' : 'IFNULL(bill_to.telephone,"")',
                'shipto_firstname' : 'IFNULL(ship_to.firstname,"")',
                'shipto_lastname' : 'IFNULL(ship_to.lastname,"")',
                'shipto_companyname' : 'IFNULL(ship_to.company,"")',
                'shipto_address' : 'IFNULL(ship_to.street,"")',
                'shipto_city' : 'IFNULL(ship_to.city,"")',
                'shipto_region' : 'IFNULL(ship_to_region.code,"")',
                'shipto_country' : 'IFNULL(ship_to.country_id,"")',
                'shipto_postcode' : 'IFNULL(ship_to.postcode,"")',
                'shipto_telephone' : 'IFNULL(ship_to.telephone,"")',
                'total_qty' : 'IFNULL(sales_order.total_qty_ordered,0)',
                'sub_total' : 'IFNULL(sales_order.subtotal,0)',
                'discount_amt' : 'IFNULL(sales_order.discount_amount,0)',
                'shipping_amt' : 'IFNULL(sales_order.shipping_amount,0)',
                'tax_amt' : 'IFNULL(sales_order.tax_amount,0)',
                'giftcard_amt' : '0',
                'storecredit_amt' : '0',
                'grand_total' : 'sales_order.grand_total',
                'coupon_code' : 'sales_order.coupon_code',
                'shipping_tax_amt' : 'IFNULL(sales_order.shipping_tax_amount,0)',
                'payment_method' : "'checkmo'",
            },
            'source_tables' : """
                FROM
                sales_order
                LEFT JOIN sales_order_address bill_to on (sales_order.entity_id = bill_to.parent_id and bill_to.address_type = 'billing')
                LEFT JOIN sales_order_address ship_to on (sales_order.entity_id = ship_to.parent_id and ship_to.address_type = 'shipping')
                LEFT JOIN directory_country_region bill_to_region on (bill_to.region_id = bill_to_region.region_id and bill_to.country_id = bill_to_region.country_id)
                LEFT JOIN directory_country_region ship_to_region on (ship_to.region_id = ship_to_region.region_id and ship_to.country_id = ship_to_region.country_id)
                LEFT JOIN customer_entity customer on sales_order.customer_id = customer.entity_id
                LEFT JOIN customer_group customer_group on customer.group_id = customer_group.customer_group_id
            """,
            'wheres' : '',
            'order_by' : """
                ORDER BY sales_order.entity_id
            """
        }

        self._orderItemQueryMap = {
            'fields' : {
                "id" : "sales_order_item.item_id",
                "m_order_id" : "sales_order_item.order_id",
                "sku" : "sales_order_item.sku",
                "name" : "sales_order_item.name",
                "uom" : "''",
                "original_price" : "sales_order_item.original_price",
                "price" : "sales_order_item.price",
                "discount_amt" : "sales_order_item.discount_amount",
                "tax_amt" : "sales_order_item.tax_amount",
                "qty" : "sales_order_item.qty_ordered",
                "sub_total" : "sales_order_item.row_total"
            },
            'source_tables' : """
                FROM
                    sales_order_item
            """,
            'wheres' : """
                WHERE
                    parent_item_id is null and
                    order_id = %s
            """
        }

        self.PRODUCTEXPORTHEADER = [
           {
              "column":"sku",
              "table":"Default",
              "frontend":"mage2",
              "metadata":{
                 "source":"sku",
                 "destination":"sku",
                 "required":True,
                 "type":"unicode"
              }
           },
           {
              "column":"short_description",
              "table":"Default",
              "frontend":"mage2",
              "metadata":{
                 "source":"short_desc",
                 "destination":"short_description",
                 "required":True,
                 "type":"unicode",
                 "default": "short_description"
              }
           },
           {
              "column":"description",
              "table":"Default",
              "frontend":"mage2",
              "metadata":{
                 "source":"description",
                 "destination":"description",
                 "required":True,
                 "type":"unicode",
                 "default": "description"
              }
           },
           {
              "column":"name",
              "table":"Default",
              "frontend":"mage2",
              "metadata":{
                 "source":"name",
                 "destination":"name",
                 "required":True,
                 "type":"unicode",
                 "default": "name"
              }
           }
        ]
        self.inventory = {
            "warehouse": None,
            "on_hand": 0,
            "past_on_hand": 0,
            "qty": 0,
            "full": True,
            "in_stock": False
        }

    def _generateGetSQL(self, queryMap):
        sql = "SELECT "
        selects = []
        for alias in sorted(queryMap['fields']) :
            column = queryMap['fields'][alias]
            selects.append("{0} as {1}".format(column,alias))
        select = ", ".join(selects)
        sql = sql + select + " " + queryMap['source_tables']
        if 'wheres' in queryMap:
            sql = sql +  " " +  queryMap['wheres']
        if 'order_by' in queryMap:
            sql = sql +  " " +  queryMap['order_by']
        return sql

    def _convertOrders(self, rawOrders):
        orders = []
        for _order in rawOrders:
            items = []
            for _item in _order['items']:
                item = {
                    'sku': _item['sku'],
                    'name': _item['name'],
                    'uom': _item['uom'],
                    'original_price': str(_item['original_price']),
                    'price': str(_item['price']),
                    'discount_amt': str(_item['discount_amt']),
                    'tax_amt': str(_item['tax_amt']),
                    'qty': str(_item['qty']),
                    'sub_total': str(_item['sub_total']),
                }
                item = {k: v for k, v in item.items() if v not in (None, "None", "")}
                items.append(item)
            order = {
                'bo_order_id': "",
                'fe_order_id': _order['m_order_inc_id'],
                'fe_order_date': _order['m_order_date'].strftime("%Y-%m-%d %H:%M:%S"),
                'fe_order_update_date': _order['m_order_update_date'].strftime("%Y-%m-%d %H:%M:%S"),
                'fe_order_status': _order['m_order_status'],
                'fe_store_id': _order['m_store_id'],
                'fe_customer_id': _order['m_customer_id'],
                'fe_customer_group': _order['m_customer_group'],
                'shipment_carrier': _order['shipment_carrier'],
                'shipment_method': _order['shipment_method'],
                'total_qty': str(_order['total_qty']),
                'sub_total': str(_order['sub_total']),
                'discount_amt': str(_order['discount_amt']),
                'shipping_amt': str(_order['shipping_amt']),
                'tax_amt': str(_order['tax_amt']),
                'giftcard_amt': str(_order['giftcard_amt']),
                'storecredit_amt': str(_order['storecredit_amt']),
                'grand_total': str(_order['grand_total']),
                'coupon_code': _order['coupon_code'],
                'shipping_tax_amt': str(_order['shipping_tax_amt']),
                'payment_method': _order['payment_method'],
                'addresses': {
                    'billto': {
                      'firstname': _order['billto_firstname'],
                      'lastname': _order['billto_lastname'],
                      'email': _order['billto_email'],
                      'companyname': _order['billto_companyname'],
                      'address': _order['billto_address'],
                      'city': _order['billto_city'],
                      'region': _order['billto_region'],
                      'country': _order['billto_country'],
                      'postcode': _order['billto_postcode'],
                      'telephone': _order['billto_telephone'],
                    },
                    'shipto': {
                        'firstname': _order['shipto_firstname'],
                        'lastname': _order['shipto_lastname'],
                        'companyname': _order['shipto_companyname'],
                        'address': _order['shipto_address'],
                        'city': _order['shipto_city'],
                        'region': _order['shipto_region'],
                        'country': _order['shipto_country'],
                        'postcode': _order['shipto_postcode'],
                        'telephone': _order['shipto_telephone'],
                    }
                },
                'items': items
            }
            order = {k: v for k, v in order.items() if v not in (None, "None", "")}
            order["addresses"]["billto"] = {k: v for k, v in order["addresses"]["billto"].items() if v not in (None, "None", "")}
            order["addresses"]["shipto"] = {k: v for k, v in order["addresses"]["shipto"].items() if v not in (None, "None", "")}
            orders.append(order)
        return orders

    def setInventory(self, cutdt,offset=None,limit=None):
        try:
            rawData = self.mage1.getInventory(cutdt,offset,limit) # Develop the function.
            productsExtData = {}
            metaData = dict((k, list(set(map(lambda d: d[k], rawData)))) for k in  ['sku', 'website_code'])
            for sku in metaData["sku"]:
                inventories = []
                for warehouse in metaData["website_code"]:
                    rows = list(filter(lambda t: (t["sku"]==sku and t["website_code"]==warehouse), rawData))
                    if len(rows) != 0:
                        entity = rows[0]
                        inventory = copy.deepcopy(self.inventory)
                        inventory["warehouse"] = warehouse
                        inventory["qty"] = entity["qty"]
                        if "full" in entity.keys():
                            inventory["full"] = entity["full"]
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

    def setImageGallery(self, cutDt, offset=None, limit=None):
        try:
            rawDataImage = self.mage1.getImages(cutDt, offset=offset, limit=limit)
            rawDataGallery = self.mage1.getGallery(cutDt, offset=offset, limit=limit)
            metaData = dict((k, list(set(map(lambda d: d[k], rawDataImage)))) for k in ['sku'])

            productsExtData = {}
            for sku in metaData["sku"]:
                rows = list(filter(lambda t: (t["sku"]==sku), rawDataImage))
                imageGallery = {}
                for row in rows:
                    imageGallery[row["type"]] = row["value"]

                imageGallery["media_gallery"] = [
                    {
                        "lable": row["label"] if row["label"] is not None else "####",
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

    def feOrdersFt(self, cutdt):
        orders = []
        orderQueryMap = copy.deepcopy(self._orderQueryMap)
        if 'wheres' in orderQueryMap and orderQueryMap['wheres'].strip() != '':
            wheres = orderQueryMap['wheres'] + " AND "
        else :
            wheres = ' WHERE '
        wheres = wheres + " sales_order.updated_at > %s "
        orderQueryMap['wheres'] = wheres
        paramValues = [cutdt]
        getOrderQuery = self._generateGetSQL(orderQueryMap)
        cursor = self.mage1.adaptor.mySQLCursor
        cursor.execute(getOrderQuery,paramValues)
        rawOrders = cursor.fetchall()
        getOrderItemQuery = self._generateGetSQL(self._orderItemQueryMap)
        for order in rawOrders:
            cursor.execute(getOrderItemQuery,[order['id']])
            order['items'] = cursor.fetchall()
        orders = self._convertOrders(rawOrders)
        return (orders, rawOrders)

    def feOrdersExtFt(self, orders, rawOrders):
        pass

    # Sync Products from BackOffice to FrontEnd.
    def syncProductFt(self, product):
        sku = product['sku']
        attributeSet = product['table']
        data = product['data']
        typeId = data.pop('type_id', 'simple')
        storeId = data.pop('store_id', '0')
        try:
            product['fe_product_id'] = self.mage1.syncProduct(sku, attributeSet, data, typeId, storeId)
        except Exception as e:
            raise

    def boOrderFt(self, order):
        if 'fe_order_id' not in order and 'order_id' in order:
            order['fe_order_id'] = order['order_id']
        if 'id' not in order and 'order_id' in order:
            order['id'] = order['order_id']
        return order

    def insertOrdersFt(self, newOrders):
        boOrders = self.mage1.insertOrders(newOrders)
        for boOrder in boOrders:
            if boOrder['tx_status'] == 'F':
                log = "Fail to insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.error(log)
                self.logger.error(boOrder['tx_note'])
            else:
                log = "Successfully insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.info(log)
        return boOrders

    def boProductsFt(self, frontend, table, offset, limit, cutDt=None):
        """We could add validation into this function.
        """
        products = []
        headers = self.getMetadata(frontend, table)
        if len(headers) == 0:
            headers = self.PRODUCTEXPORTHEADER
        self.logger.info(headers)
        attributes = []
        for header in headers:
            attributes.append(header["metadata"]["source"])
        self.rawProducts = self.mage1.getProducts(table,cutDt,attributes,limit,offset)
        for rawProduct in self.rawProducts:
            self.logger.info(rawProduct)
            data = {}
            try:
                for header in headers:
                    try:
                        metadata = header["metadata"]
                        source = metadata["source"]
                        destination = metadata["destination"]
                        required = metadata["required"]
                        dataType = metadata["type"]
                        if source in rawProduct.keys():
                            data[destination] = rawProduct[source]
                        else:
                            if required:
                                data[destination] = metadata["default"]
                    except Exception as e:
                        self.logger.error(header)
                        raise
                product = {}
                product["sku"] = data['sku']
                product["frontend"] = frontend
                product["table"] = table
                product["raw_data"] = rawProduct
                product["data"] = data
                product["create_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                product["update_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            except Exception as e:
                self.logger.error(rawProduct)
                raise

            products.append(product)
        return (products, self.rawProducts)

    def boProductsExtFt(self, products, rawProducts):
        pass

    def boProductsExtDataFt(self, frontend, dataType, offset, limit, cutDt=None):
        productsExtData = []
        rawProductsExtData = {}
        if dataType == "inventory":
            rawProductsExtData = self.setInventory(cutDt,offset,limit)
        elif dataType == "imagegallery":
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
            except Exception as e:
                error = {
                    "data_type": dataType,
                    "sku": sku,
                    "data": data
                }
                self.logger.error(json.dumps(error))
                raise

            productsExtData.append(productExtData)
        return (productsExtData, rawProductsExtData)

    def boProductsExtDataExtFt(self, dataType, productsExtData, rawProductsExtData):
        pass

    def boProductsTotalFt(self, cutDt):
        totalCount = self.mage1.getTotalProductsCount(cutDt)
        return totalCount

    def boProductsExtDataTotalFt(self, dataType, cutDt):
        totalCount = 100 # Default to handle total 1000 records
        if dataType == "inventory":
            totalCount = self.mage1.getTotalInventoryCount(cutDt)
        elif dataType == "imagegallery":
            totalCount = self.mage1.getTotalProductsCount(cutDt)
        return totalCount

    # Sync Invoices from BackOffice to FrontEnd.
    def syncInvoiceFt(self, invoice):
        invoice["fe_invoice_id"] = self.mage1.insertInvoice(invoice)
