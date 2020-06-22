#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, os, traceback, sys
from decimal import Decimal

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

from models import InvoicesModel, PurchaseOrdersModel, ProductsModel, ProductsCustomOptionModel, \
ProductsInventoryModel, ProductsImageGalleryModel, ProductsLinksModel, ProductsCategoriesModel, \
ProductsPriceLevelsModel, ProductsVariantsModel, CustomersModel, ShipmentsModel

invoicesModel = InvoicesModel()
purchaseOrdersModel = PurchaseOrdersModel()
productsModel = ProductsModel()
productsCustomOptionModel = ProductsCustomOptionModel()
productsInventoryModel = ProductsInventoryModel()
productsImageGalleryModel = ProductsImageGalleryModel()
productsLinksModel = ProductsLinksModel()
productsCategoriesModel = ProductsCategoriesModel()
productsPriceLevelsModel = ProductsPriceLevelsModel()
productsVariantsModel = ProductsVariantsModel()
customersModel = CustomersModel()
shipmentsModel = ShipmentsModel()

def handler(event, context):
    # TODO implement
    try:
        function = event["pathParameters"]["proxy"]
        if function == "invoice" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            boInvoiceId = event["queryStringParameters"]["boinvoiceid"]
            invoice = json.loads(event["body"])
            return invoicesModel.insertInvoice(backoffice, frontend, boInvoiceId, invoice)
        elif function == "invoicestatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            invoiceStatus = json.loads(event["body"])
            return invoicesModel.updateInvoiceStatus(id, invoiceStatus)
        elif function == "invoice" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            boInvoiceId = event["queryStringParameters"]["boinvoiceid"]
            return invoicesModel.getInvoice(frontend, boInvoiceId)
        elif function == "customer" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            boCustomerId = event["queryStringParameters"]["bocustomerid"]
            customer = json.loads(event["body"])
            return customersModel.insertCustomer(backoffice, frontend, boCustomerId, customer)
        elif function == "customerstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            customerStatus = json.loads(event["body"])
            return customersModel.updateCustomerStatus(id, customerStatus)
        elif function == "customer" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            boCustomerId = event["queryStringParameters"]["bocustomerid"]
            return customersModel.getCustomer(frontend, boCustomerId)
        elif function == "shipment" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            boShipmentId = event["queryStringParameters"]["boshipmentid"]
            shipment = json.loads(event["body"])
            return shipmentsModel.insertShipment(backoffice, frontend, boShipmentId, shipment)
        elif function == "shipmentstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            shipmentStatus = json.loads(event["body"])
            return shipmentsModel.updateShipmentStatus(id, shipmentStatus)
        elif function == "shipment" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            boShipmentId = event["queryStringParameters"]["boshipmentid"]
            return shipmentsModel.getShipment(frontend, boShipmentId)
        elif function == "purchaseorder" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            boPONum = event["queryStringParameters"]["boponum"]
            purchaseOrder = json.loads(event["body"], parse_float=Decimal)
            return purchaseOrdersModel.insertPurchaseOrder(backoffice, frontend, boPONum, purchaseOrder)
        elif function == "purchaseorderstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            purchaseOrderStatus = json.loads(event["body"], parse_float=Decimal)
            return purchaseOrdersModel.updatePurchaseOrderStatus(id, purchaseOrderStatus)
        elif function == "purchaseorder" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            boPONum = event["queryStringParameters"]["boponum"]
            return purchaseOrdersModel.getPurchaseOrder(frontend, boPONum)
        elif function == "product" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            sku = event["queryStringParameters"]["sku"]
            product = json.loads(json.loads(event["body"]), parse_float=Decimal)
            return productsModel.insertProduct(backoffice, frontend, sku, product)
        elif function == "productstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            productStatus = json.loads(event["body"])
            return productsModel.updateProductStatus(id, productStatus)
        elif function == "product" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            sku = event["queryStringParameters"]["sku"]
            return productsModel.getProduct(frontend, sku)
        elif function == "productextdata" and event["httpMethod"] == "PUT":
            backoffice = frontend = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            sku = event["queryStringParameters"]["sku"]
            dataType = event["queryStringParameters"]["datatype"]
            productextdata = json.loads(event["body"], parse_float=Decimal)
            if dataType == "customoption":
                return productsCustomOptionModel.insertProductCustomOption(backoffice, frontend, sku, productextdata)
            elif dataType == "inventory":
                return productsInventoryModel.insertProductInventory(backoffice, frontend, sku, productextdata)
            elif dataType == "imagegallery":
                return productsImageGalleryModel.insertProductImageGallery(backoffice, frontend, sku, productextdata)
            elif dataType == "links":
                return productsLinksModel.insertProductLinks(backoffice, frontend, sku, productextdata)
            elif dataType == "categories":
                return productsCategoriesModel.insertProductCategories(backoffice, frontend, sku, productextdata)
            elif dataType == "pricelevels":
                return productsPriceLevelsModel.insertProductPriceLevels(backoffice, frontend, sku, productextdata)
            elif dataType == "variants":
                return productsVariantsModel.insertProductVariants(backoffice, frontend, sku, productextdata)
        elif function == "productextdatastatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            productExtDataStatus = json.loads(event["body"])
            dataType = event["queryStringParameters"]["datatype"]
            if dataType == "customoption":
                return productsCustomOptionModel.updateProductCustomOptionStatus(id, productExtDataStatus)
            elif dataType == "inventory":
                return productsInventoryModel.updateProductInventoryStatus(id, productExtDataStatus)
            elif dataType == "imagegallery":
                return productsImageGalleryModel.updateProductImageGalleryStatus(id, productExtDataStatus)
            elif dataType == "links":
                return productsLinksModel.updateProductLinksStatus(id, productExtDataStatus)
            elif dataType == "categories":
                return productsCategoriesModel.updateProductCategoriesStatus(id, productExtDataStatus)
            elif dataType == "pricelevels":
                return productsPriceLevelsModel.updateProductPriceLevelsStatus(id, productExtDataStatus)
            elif dataType == "variants":
                return productsVariantsModel.updateProductVariantsStatus(id, productExtDataStatus)
        elif function == "productextdata" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            sku = event["queryStringParameters"]["sku"]
            dataType = event["queryStringParameters"]["datatype"]
            if dataType == "customoption":
                return productsCustomOptionModel.getProductCustomOption(frontend, sku)
            elif dataType == "inventory":
                return productsInventoryModel.getProductInventory(frontend, sku)
            elif dataType == "imagegallery":
                return productsImageGalleryModel.getProductImageGallery(frontend, sku)
            elif dataType == "links":
                return productsLinksModel.getProductLinks(frontend, sku)
            elif dataType == "categories":
                return productsCategoriesModel.getProductCategories(frontend, sku)
            elif dataType == "pricelevels":
                return productsPriceLevelsModel.getProductPriceLevels(frontend, sku)
            elif dataType == "variants":
                return productsVariantsModel.getProductVariants(frontend, sku)
    except Exception as e:
        log = traceback.format_exc()
        logger.exception(log)
        return {
            "statusCode": 500,
            "headers": {},
            "body": (
                json.dumps({"error": "{0}".format(log)}, indent=4)
            )
        }
