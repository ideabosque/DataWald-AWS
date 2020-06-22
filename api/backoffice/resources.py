#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, os, traceback, sys

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

from decimal import Decimal
from models import OrdersModel, ItemReceiptsModel, CustomersModel
ordersModel = OrdersModel()
itemReceiptsModel = ItemReceiptsModel()
customersModel = CustomersModel()

def handler(event, context):
    # TODO implement
    try:
        function = event["pathParameters"]["proxy"]
        if function == "order" and event["httpMethod"] == "PUT":
            frontend = event["queryStringParameters"]["frontend"]
            feOrderId = event["queryStringParameters"]["feorderid"]
            order = json.loads(event["body"], parse_float=Decimal)
            return ordersModel.insertOrder(frontend, feOrderId, order)
        elif function == "orderstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            orderStatus = json.loads(event["body"])
            return ordersModel.updateOrderStatus(id, orderStatus)
        elif function == "order" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            feOrderId = event["queryStringParameters"]["feorderid"]
            return ordersModel.getOrder(frontend, feOrderId)
        elif function == "itemreceipt" and event["httpMethod"] == "PUT":
            frontend = event["queryStringParameters"]["frontend"]
            boPONum = event["queryStringParameters"]["boponum"]
            itemReceipt = json.loads(event["body"], parse_float=Decimal)
            return itemReceiptsModel.insertItemReceipt(frontend, boPONum, itemReceipt)
        elif function == "itemreceiptstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            itemReceiptStatus = json.loads(event["body"])
            return itemReceiptsModel.updateItemReceiptStatus(id, itemReceiptStatus)
        elif function == "itemreceipt" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            boPONum = event["queryStringParameters"]["boponum"]
            return itemReceiptsModel.getItemReceipt(frontend, boPONum)
        elif function == "customer" and event["httpMethod"] == "PUT":
            frontend = event["queryStringParameters"]["frontend"]
            feCustomerId = event["queryStringParameters"]["fecustomerid"]
            customer = json.loads(event["body"], parse_float=Decimal)
            return customersModel.insertCustomer(frontend, feCustomerId, customer)
        elif function == "customerstatus" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            customerStatus = json.loads(event["body"])
            return customersModel.updateCustomerStatus(id, customerStatus)
        elif function == "customer" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            feCustomerId = event["queryStringParameters"]["fecustomerid"]
            return customersModel.getCustomer(frontend, feCustomerId)
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
