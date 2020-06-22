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

configData = dynamodb.Table('config_data')
response = configData.get_item(
    Key={
        'key': "BACKOFFICEAPI"
    }
)
BACKOFFICEAPI = response["Item"]["value"]

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


class OrdersModel(object):

    def __init__(self):
        self._orders = dynamodb.Table('orders')

    @property
    def orders(self):
        return self._orders

    def _getOrder(self, frontend, feOrderId):
        response = self.orders.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('fe_order_id').eq(feOrderId),
            Limit=1
        )
        return response

    def getOrders(self):
        pass

    def getOrder(self, frontend, feOrderId):
        order = {}
        response = self._getOrder(frontend, feOrderId)
        if response['Count'] != 0:
            order = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(order, indent=4, cls=JSONEncoder))
        }

    def insertOrder(self, frontend, feOrderId, order):
        insertStatus = BACKOFFICEAPI['DWFEORDERSTATUS_METRICS']['insert']['status']
        order['tx_status'] = order.get("tx_status", "N") if order['fe_order_status'].lower() in insertStatus else "I"
        order['create_dt'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        order['tx_dt'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        order['tx_note'] = '{0} -> DataWald'.format(frontend)
        order['frontend'] = frontend

        response = self._getOrder(frontend, feOrderId)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if order['fe_order_status'] != item['fe_order_status']:
                order["id"] = _id
            else:
                if item["tx_status"] == "N":
                    order = item
                    order["tx_status"] = "P"
                elif item["tx_status"] == "F" and order["tx_status"] == "N":
                    order["id"] = _id
                else:
                    order = item
            self.orders.put_item(Item=order)
            log = "Successfully update document: {0}/{1}".format(order["fe_order_id"], order["id"])
            logger.info(log)
        else:
            order["id"] = _id
            self.orders.put_item(Item=order)
            log = "Successfully insert document: {0}/{1}".format(order["fe_order_id"], order["id"])
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "fe_order_id": feOrderId
            })
        }

    def updateOrderStatus(self, id, orderStatus):
        response = self.orders.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set bo_order_id=:val0, tx_dt=:val1, tx_status=:val2, tx_note=:val3",
            ExpressionAttributeValues={
                ':val0': orderStatus['bo_order_id'],
                ':val1': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val2': orderStatus['tx_status'],
                ':val3': orderStatus['tx_note']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }


class ItemReceiptsModel(object):

    def __init__(self):
        self._itemReceipts = dynamodb.Table('itemreceipts')

    @property
    def itemReceipts(self):
        return self._itemReceipts

    def _getItemReceipt(self, frontend, boPONum):
        response = self.itemReceipts.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('bo_po_num').eq(boPONum),
            Limit=1
        )
        return response

    def getItemReceipts(self):
        pass

    def getItemReceipt(self, frontend, boPONum):
        itemReceipt = {}
        response = self._getItemReceipt(frontend, boPONum)
        if response['Count'] != 0:
            itemReceipt = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(itemReceipt, indent=4, cls=JSONEncoder))
        }

    def insertItemReceipt(self, frontend, boPONum, itemReceipt):
        itemReceipt["frontend"] = frontend
        itemReceipt["tx_status"] = itemReceipt.get("tx_status", "N")
        itemReceipt["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        itemReceipt["tx_note"] = '{0} -> DataWald'.format(frontend)

        response = self._getItemReceipt(frontend, boPONum)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if itemReceipt['data'] != item['data']:
                history = {}
                if 'history' in item.keys():
                    history = item['history']
                createDt = item["create_dt"]
                history[createDt] = item['data']
                itemReceipt['history'] = history
                itemReceipt["id"] = _id
                itemReceipt["bo_itemreceipt_id"] = item["bo_itemreceipt_id"]
                self.itemReceipts.put_item(Item=itemReceipt)
                log = "Successfully update item recepit: {0}/{1}".format(frontend, boPONum)
                logger.info(log)
            else:
                log = "No update item recepit: {0}/{1}".format(frontend, boPONum)
                logger.info(log)
                response = self.itemReceipts.update_item(
                    Key={
                        'id': _id
                    },
                    UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2",
                    ExpressionAttributeValues={
                        ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                        ':val1': "I",
                        ':val2': log
                    },
                    ReturnValues="UPDATED_NEW"
                )
        else:
            itemReceipt["id"] = _id
            self.itemReceipts.put_item(Item=itemReceipt)
            log = "Successfully insert item recepit: {0}/{1}".format(frontend, boPONum)
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

    def updateItemReceiptStatus(self, id, itemReceiptStatus):
        response = self.itemReceipts.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set tx_dt=:val0, tx_status=:val1, tx_note=:val2, bo_itemreceipt_id=:val3",
            ExpressionAttributeValues={
                ':val0': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val1': itemReceiptStatus['tx_status'],
                ':val2': itemReceiptStatus['tx_note'],
                ':val3': itemReceiptStatus['bo_itemreceipt_id']
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
        self._customers = dynamodb.Table('customers-bo')

    @property
    def customers(self):
        return self._customers

    def _getCustomer(self, frontend, feCustomerId):
        response = self.customers.query(
            IndexName="frontend_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('fe_customer_id').eq(feCustomerId),
            Limit=1
        )
        return response

    def getCustomers(self):
        pass

    def getCustomer(self, frontend, feCustomerId):
        customer = {}
        response = self._getCustomer(frontend, feCustomerId)
        if response['Count'] != 0:
            customer = response["Items"][0]
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(customer, indent=4, cls=JSONEncoder))
        }

    def insertCustomer(self, frontend, feCustomerId, customer):
        customer['tx_status'] = customer.get("tx_status", "N")
        customer['tx_dt'] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        customer['tx_note'] = '{0} -> DataWald'.format(frontend)
        customer['frontend'] = frontend

        response = self._getCustomer(frontend, feCustomerId)
        _id = str(uuid.uuid1())
        if response['Count'] != 0:
            item = response["Items"][0]
            _id = item["id"]
            if customer['data'] != item['data']:
                createDt = item["create_dt"]
                customer["id"] = _id
                customer["create_dt"] = createDt
                self.customers.put_item(Item=customer)
                log = "Successfully update customer: {0}/{1}".format(frontend, feCustomerId)
                logger.info(log)
            else:
                log = "No update customer: {0}/{1}".format(frontend, feCustomerId)
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
            log = "Successfully insert customer: {0}/{1}".format(frontend, feCustomerId)
            logger.info(log)

        return {
            "statusCode": 200,
            "headers": {},
            "body": json.dumps({
                "id": _id,
                "frontend": frontend,
                "fe_customer_id": feCustomerId
            })
        }

    def updateCustomerStatus(self, id, customerStatus):
        response = self.customers.update_item(
            Key={
                'id': id
            },
            UpdateExpression="set fe_customer_id=:val0, tx_dt=:val1, tx_status=:val2, tx_note=:val3",
            ExpressionAttributeValues={
                ':val0': customerStatus['fe_customer_id'],
                ':val1': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val2': customerStatus['tx_status'],
                ':val3': customerStatus['tx_note']
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }
