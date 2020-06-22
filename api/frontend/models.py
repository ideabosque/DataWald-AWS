#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, uuid, os
from datetime import datetime, date
from decimal import Decimal

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

import boto3
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')

configData = dynamodb.Table('config_data')
response = configData.get_item(
    Key={
        'key': "FRONTENDAPI"
    }
)
FRONTENDAPI = response["Item"]["value"]

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


class InvoicesModel(object):

    def __init__(self):
        self._invoices = dynamodb.Table('invoices')

    @property
    def invoices(self):
        return self._invoices

    def _getInvoice(self, frontend, boInvoiceId):
        response = self.invoices.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('bo_invoice_id').eq(boInvoiceId),
            Limit=1
        )
        return response

    def getInvoices(self):
        pass

    def getInvoice(self, frontend, boInvoiceId):
        invoice = {}
        response = self._getInvoice(frontend, boInvoiceId)
        if response['Count'] != 0:
            invoice = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(invoice, indent=4, cls=JSONEncoder))
        }

    def insertInvoice(self, backoffice, frontend, boInvoiceId, invoice):
        invoice["tx_status"] = invoice.get("tx_status", "N")
        invoice["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        invoice["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getInvoice(frontend, boInvoiceId)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if invoice['data'] != item['data']:
                createDt = item["create_dt"]
                invoice["id"] = _id
                invoice["create_dt"] = createDt
                self.invoices.put_item(Item=invoice)
                log = "Successfully update invoice: {0}/{1}".format(frontend, boInvoiceId)
                logger.info(log)
            else:
                log = "No update invoice: {0}/{1}".format(frontend, boInvoiceId)
                logger.info(log)
                response = self.invoices.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            invoice["id"] = _id
            self.invoices.put_item(Item=invoice)
            log = "Successfully insert invoice: {0}/{1}".format(frontend, boInvoiceId)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "bo_invoice_id": boInvoiceId
            })
        }

    def updateInvoiceStatus(self, id, invoiceStatus):
        response = self.invoices.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_invoice_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': invoiceStatus['tx_status'],
                ':val2': invoiceStatus['tx_note'],
                ':val3': invoiceStatus['fe_invoice_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class PurchaseOrdersModel(object):

    def __init__(self):
        self._purchaseOrders = dynamodb.Table('purchaseorders')

    @property
    def purchaseOrders(self):
        return self._purchaseOrders

    def _getPurchaseOrder(self, frontend, boPONum):
        response = self.purchaseOrders.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('bo_po_num').eq(boPONum),
            Limit=1
        )
        return response

    def getPurchaseOrders(self):
        pass

    def getPurchaseOrder(self, frontend, boPONum):
        purchaseOrder = {}
        response = self._getPurchaseOrder(frontend, boPONum)
        if response['Count'] != 0:
            purchaseOrder = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(purchaseOrder, indent=4, cls=JSONEncoder))
        }

    def insertPurchaseOrder(self, backoffice, frontend, boPONum, purchaseOrder):
        purchaseOrder["tx_status"] = purchaseOrder.get("tx_status", "N")
        purchaseOrder["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        purchaseOrder["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getPurchaseOrder(frontend, boPONum)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if purchaseOrder['data'] != item['data']:
                createDt = item["create_dt"]
                purchaseOrder["id"] = _id
                purchaseOrder["create_dt"] = createDt
                self.purchaseOrders.put_item(Item=purchaseOrder)
                log = "Successfully update purchase order: {0}/{1}".format(frontend, boPONum)
                logger.info(log)
            else:
                log = "No update purchase order: {0}/{1}".format(frontend, boPONum)
                logger.info(log)
                response = self.purchaseOrders.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            purchaseOrder["id"] = _id
            self.purchaseOrders.put_item(Item=purchaseOrder)
            log = "Successfully insert purchase order: {0}/{1}".format(frontend, boPONum)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "bo_po_num": boPONum
            })
        }

    def updatePurchaseOrderStatus(self, id, purchaseOrderStatus):
        response = self.purchaseOrders.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_po_num=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': purchaseOrderStatus['tx_status'],
                ':val2': purchaseOrderStatus['tx_note'],
                ':val3': purchaseOrderStatus['fe_po_num']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsModel(object):

    def __init__(self):
        self._products = dynamodb.Table('products')

    @property
    def products(self):
        return self._products

    def _getProduct(self, frontend, sku):
        response = self.products.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProducts(self):
        pass

    def getProduct(self, frontend, sku):
        product = {}
        response = self._getProduct(frontend, sku)
        if response['Count'] != 0:
            product = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(product, indent=4, cls=JSONEncoder))
        }

    def insertProduct(self, backoffice, frontend, sku, product):
        product["tx_status"] = product.get("tx_status", "N")
        product["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        product["tx_note"] = '{0} -> DataWald'.format(backoffice)
        product["old_data"] = {}

        response = self._getProduct(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if product['data'] != item['data']:
                createDt = item["create_dt"]
                product["id"] = _id
                for k,v in item["data"].items():
                    if v != product["data"].get(k, None):
                        product["old_data"][k] = v
                product["create_dt"] = createDt
                self.products.put_item(Item=product)
                log = "Successfully update product: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.products.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            product["id"] = _id
            self.products.put_item(Item=product)
            log = "Successfully insert product: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductStatus(self, id, productStatus):
        response = self.products.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productStatus['tx_status'],
                ':val2': productStatus['tx_note'],
                ':val3': productStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsCustomOptionModel(object):

    def __init__(self):
        self._productsCustomOption = dynamodb.Table('products-customoption')

    @property
    def productsCustomOption(self):
        return self._productsCustomOption

    def _getProductCustomOption(self, frontend, sku):
        response = self.productsCustomOption.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsCustomOption(self):
        pass

    def getProductCustomOption(self, frontend, sku):
        productCustomOption = {}
        response = self._getProductCustomOption(frontend, sku)
        if response['Count'] != 0:
            productCustomOption = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productCustomOption, indent=4, cls=JSONEncoder))
        }

    def insertProductCustomOption(self, backoffice, frontend, sku, productCustomOption):
        productCustomOption["tx_status"] = productCustomOption.get("tx_status", "N")
        productCustomOption["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productCustomOption["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductCustomOption(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productCustomOption['data'] != item['data']:
                createDt = item["create_dt"]
                productCustomOption["id"] = _id
                productCustomOption["create_dt"] = createDt
                self.productsCustomOption.put_item(Item=productCustomOption)
                log = "Successfully update product custom option: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product custom option: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsCustomOption.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productCustomOption["id"] = _id
            self.productsCustomOption.put_item(Item=productCustomOption)
            log = "Successfully insert product custom option: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductCustomOptionStatus(self, id, productCustomOptionStatus):
        response = self.productsCustomOption.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productCustomOptionStatus['tx_status'],
                ':val2': productCustomOptionStatus['tx_note'],
                ':val3': productCustomOptionStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsInventoryModel(object):

    def __init__(self):
        self._productsInventory = dynamodb.Table('products-inventory')

    @property
    def productsInventory(self):
        return self._productsInventory

    def _getProductInventory(self, frontend, sku):
        response = self.productsInventory.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsInventory(self):
        pass

    def getProductInventory(self, frontend, sku):
        productInventory = {}
        response = self._getProductInventory(frontend, sku)
        if response['Count'] != 0:
            productInventory = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productInventory, indent=4, cls=JSONEncoder))
        }

    def setInventory(self, inData, data):
        for line in inData:
            rows = list(filter(lambda t: (t["warehouse"]==line["warehouse"]), data))
            if len(rows) > 0:
                line["past_on_hand"] = rows[0]["on_hand"]

            if not line["full"]:
                line["on_hand"] = line["past_on_hand"] + line["qty"]
            else:
                line["on_hand"] = line["qty"]

            if line["on_hand"] > 0:
                line["in_stock"] = True

    def insertProductInventory(self, backoffice, frontend, sku, productInventory):
        productInventory["tx_status"] = productInventory.get("tx_status", "N")
        productInventory["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productInventory["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductInventory(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productInventory['data'] != item['data']:
                self.setInventory(productInventory["data"], item["data"])
                createDt = item["create_dt"]
                productInventory["id"] = _id
                productInventory["create_dt"] = createDt
                self.productsInventory.put_item(Item=productInventory)
                log = "Successfully update product inventory: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product inventory: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsInventory.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N",  # if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productInventory["id"] = _id
            self.productsInventory.put_item(Item=productInventory)
            log = "Successfully insert product inventory: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductInventoryStatus(self, id, productInventoryStatus):
        response = self.productsInventory.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productInventoryStatus['tx_status'],
                ':val2': productInventoryStatus['tx_note'],
                ':val3': productInventoryStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsImageGalleryModel(object):

    def __init__(self):
        self._productsImageGallery = dynamodb.Table('products-imagegallery')

    @property
    def productsImageGallery(self):
        return self._productsImageGallery

    def _getProductImageGallery(self, frontend, sku):
        response = self.productsImageGallery.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsImageGallery(self):
        pass

    def getProductImageGallery(self, frontend, sku):
        productImageGallery = {}
        response = self._getProductImageGallery(frontend, sku)
        if response['Count'] != 0:
            productImageGallery = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productImageGallery, indent=4, cls=JSONEncoder))
        }


    def insertProductImageGallery(self, backoffice, frontend, sku, productImageGallery):
        productImageGallery["tx_status"] = productImageGallery.get("tx_status", "N")
        productImageGallery["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productImageGallery["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductImageGallery(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productImageGallery['data'] != item['data']:
                createDt = item["create_dt"]
                productImageGallery["id"] = _id
                productImageGallery["create_dt"] = createDt
                self.productsImageGallery.put_item(Item=productImageGallery)
                log = "Successfully update product image gallery: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product image gallery: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsImageGallery.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productImageGallery["id"] = _id
            self.productsImageGallery.put_item(Item=productImageGallery)
            log = "Successfully insert product image gallery: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductImageGalleryStatus(self, id, productImageGalleryStatus):
        response = self.productsImageGallery.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productImageGalleryStatus['tx_status'],
                ':val2': productImageGalleryStatus['tx_note'],
                ':val3': productImageGalleryStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsLinksModel(object):

    def __init__(self):
        self._productsLinks = dynamodb.Table('products-links')

    @property
    def productsLinks(self):
        return self._productsLinks

    def _getProductLinks(self, frontend, sku):
        response = self.productsLinks.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsLinks(self):
        pass

    def getProductLinks(self, frontend, sku):
        productLinks = {}
        response = self._getProductLinks(frontend, sku)
        if response['Count'] != 0:
            productLinks = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productLinks, indent=4, cls=JSONEncoder))
        }


    def insertProductLinks(self, backoffice, frontend, sku, productLinks):
        productLinks["tx_status"] = productLinks.get("tx_status", "N")
        productLinks["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productLinks["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductLinks(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productLinks['data'] != item['data']:
                createDt = item["create_dt"]
                productLinks["id"] = _id
                productLinks["create_dt"] = createDt
                self.productsLinks.put_item(Item=productLinks)
                log = "Successfully update product links: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product links: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsLinks.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productLinks["id"] = _id
            self.productsLinks.put_item(Item=productLinks)
            log = "Successfully insert product links: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductLinksStatus(self, id, productLinksStatus):
        response = self.productsLinks.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productLinksStatus['tx_status'],
                ':val2': productLinksStatus['tx_note'],
                ':val3': productLinksStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsCategoriesModel(object):

    def __init__(self):
        self._productsCategories = dynamodb.Table('products-categories')

    @property
    def productsCategories(self):
        return self._productsCategories

    def _getProductCategories(self, frontend, sku):
        response = self.productsCategories.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsCategories(self):
        pass

    def getProductCategories(self, frontend, sku):
        productCategories = {}
        response = self._getProductCategories(frontend, sku)
        if response['Count'] != 0:
            productCategories = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productCategories, indent=4, cls=JSONEncoder))
        }


    def insertProductCategories(self, backoffice, frontend, sku, productCategories):
        productCategories["tx_status"] = productCategories.get("tx_status", "N")
        productCategories["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productCategories["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductCategories(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productCategories['data'] != item['data']:
                createDt = item["create_dt"]
                productCategories["id"] = _id
                productCategories["create_dt"] = createDt
                self.productsCategories.put_item(Item=productCategories)
                log = "Successfully update product categories: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product categories: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsCategories.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productCategories["id"] = _id
            self.productsCategories.put_item(Item=productCategories)
            log = "Successfully insert product categories: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductCategoriesStatus(self, id, productCategoriesStatus):
        response = self.productsCategories.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productCategoriesStatus['tx_status'],
                ':val2': productCategoriesStatus['tx_note'],
                ':val3': productCategoriesStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ProductsPriceLevelsModel(object):

    def __init__(self):
        self._productsPriceLevels = dynamodb.Table('products-pricelevels')

    @property
    def productsPriceLevels(self):
        return self._productsPriceLevels

    def _getProductPriceLevels(self, frontend, sku):
        response = self.productsPriceLevels.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsPriceLevels(self):
        pass

    def getProductPriceLevels(self, frontend, sku):
        productPriceLevels = {}
        response = self._getProductPriceLevels(frontend, sku)
        if response['Count'] != 0:
            productPriceLevels = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productPriceLevels, indent=4, cls=JSONEncoder))
        }


    def insertProductPriceLevels(self, backoffice, frontend, sku, productPriceLevels):
        productPriceLevels["tx_status"] = productPriceLevels.get("tx_status", "N")
        productPriceLevels["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productPriceLevels["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductPriceLevels(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productPriceLevels['data'] != item['data']:
                createDt = item["create_dt"]
                productPriceLevels["id"] = _id
                productPriceLevels["create_dt"] = createDt
                self.productsPriceLevels.put_item(Item=productPriceLevels)
                log = "Successfully update product pricelevels: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product pricelevels: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsPriceLevels.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productPriceLevels["id"] = _id
            self.productsPriceLevels.put_item(Item=productPriceLevels)
            log = "Successfully insert product pricelevels: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductPriceLevelsStatus(self, id, productPriceLevelsStatus):
        response = self.productsPriceLevels.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productPriceLevelsStatus['tx_status'],
                ':val2': productPriceLevelsStatus['tx_note'],
                ':val3': productPriceLevelsStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }

class ProductsVariantsModel(object):

    def __init__(self):
        self._productsVariants = dynamodb.Table('products-variants')

    @property
    def productsVariants(self):
        return self._productsVariants

    def _getProductVariants(self, frontend, sku):
        response = self.productsVariants.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('sku').eq(sku),
            Limit=1
        )
        return response

    def getProductsVariants(self):
        pass

    def getProductVariants(self, frontend, sku):
        productVariants = {}
        response = self._getProductVariants(frontend, sku)
        if response['Count'] != 0:
            productVariants = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(productVariants, indent=4, cls=JSONEncoder))
        }


    def insertProductVariants(self, backoffice, frontend, sku, productVariants):
        productVariants["tx_status"] = productVariants.get("tx_status", "N")
        productVariants["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        productVariants["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getProductVariants(frontend, sku)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if productVariants['data'] != item['data']:
                createDt = item["create_dt"]
                productVariants["id"] = _id
                productVariants["create_dt"] = createDt
                self.productsVariants.put_item(Item=productVariants)
                log = "Successfully update product variants: {0}/{1}".format(frontend, sku)
                logger.info(log)
            else:
                log = "No update product variants: {0}/{1}".format(frontend, sku)
                logger.info(log)
                response = self.productsVariants.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            productVariants["id"] = _id
            self.productsVariants.put_item(Item=productVariants)
            log = "Successfully insert product variants: {0}/{1}".format(frontend, sku)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "sku": sku
            })
        }

    def updateProductVariantsStatus(self, id, productVariantsStatus):
        response = self.productsVariants.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_product_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': productVariantsStatus['tx_status'],
                ':val2': productVariantsStatus['tx_note'],
                ':val3': productVariantsStatus['fe_product_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }

class CustomersModel(object):

    def __init__(self):
        self._customers = dynamodb.Table('customers-fe')

    @property
    def customers(self):
        return self._customers

    def _getCustomer(self, frontend, boCustomerId):
        response = self.customers.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('bo_customer_id').eq(boCustomerId),
            Limit=1
        )
        return response

    def getCustomers(self):
        pass

    def getCustomer(self, frontend, boCustomerId):
        customer = {}
        response = self._getCustomer(frontend, boCustomerId)
        if response['Count'] != 0:
            customer = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(customer, indent=4, cls=JSONEncoder))
        }

    def insertCustomer(self, backoffice, frontend, boCustomerId, customer):
        customer["tx_status"] = customer.get("tx_status", "N")
        customer["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        customer["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getCustomer(frontend, boCustomerId)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if customer['data'] != item['data']:
                createDt = item["create_dt"]
                customer["id"] = _id
                customer["create_dt"] = createDt
                self.customers.put_item(Item=customer)
                log = "Successfully update customer: {0}/{1}".format(frontend, boCustomerId)
                logger.info(log)
            else:
                log = "No update customer: {0}/{1}".format(frontend, boCustomerId)
                logger.info(log)
                response = self.customers.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            customer["id"] = _id
            self.customers.put_item(Item=customer)
            log = "Successfully insert customer: {0}/{1}".format(frontend, boCustomerId)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "bo_customer_id": boCustomerId
            })
        }

    def updateCustomerStatus(self, id, customerStatus):
        response = self.customers.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_customer_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': customerStatus['tx_status'],
                ':val2': customerStatus['tx_note'],
                ':val3': customerStatus['fe_customer_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ShipmentsModel(object):

    def __init__(self):
        self._shipments = dynamodb.Table('shipments')

    @property
    def shipments(self):
        return self._shipments

    def _getShipment(self, frontend, boShipmentId):
        response = self.shipments.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('bo_shipment_id').eq(boShipmentId),
            Limit=1
        )
        return response

    def getShipments(self):
        pass

    def getShipment(self, frontend, boShipmentId):
        shipment = {}
        response = self._getShipment(frontend, boShipmentId)
        if response['Count'] != 0:
            shipment = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(shipment, indent=4, cls=JSONEncoder))
        }

    def insertShipment(self, backoffice, frontend, boShipmentId, shipment):
        shipment["tx_status"] = shipment.get("tx_status", "N")
        shipment["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        shipment["tx_note"] = '{0} -> DataWald'.format(backoffice)

        response = self._getShipment(frontend, boShipmentId)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if shipment['data'] != item['data']:
                createDt = item["create_dt"]
                shipment["id"] = _id
                shipment["create_dt"] = createDt
                self.shipments.put_item(Item=shipment)
                log = "Successfully update shipment: {0}/{1}".format(frontend, boShipmentId)
                logger.info(log)
            else:
                log = "No update shipment: {0}/{1}".format(frontend, boShipmentId)
                logger.info(log)
                response = self.shipments.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "N" if item['tx_status'] in ('N', 'F') else 'I',
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            shipment["id"] = _id
            self.shipments.put_item(Item=shipment)
            log = "Successfully insert shipment: {0}/{1}".format(frontend, boShipmentId)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "bo_shipment_id": boShipmentId
            })
        }

    def updateShipmentStatus(self, id, shipmentStatus):
        response = self.shipments.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, fe_shipment_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': shipmentStatus['tx_status'],
                ':val2': shipmentStatus['tx_note'],
                ':val3': shipmentStatus['fe_shipment_id']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }
