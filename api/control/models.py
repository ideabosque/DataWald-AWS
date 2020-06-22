#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, uuid, os, traceback
from datetime import datetime, timedelta, date
from decimal import Decimal

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

import boto3
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')
sqs = boto3.resource('sqs')

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


class SyncControlModel(object):

    def __init__(self):
        self._syncControl = dynamodb.Table('sync_control')
        self.tables = {
            "orders": {
                "area": "backoffice",
                "srcKey": "fe_order_id",
                "tgtKey": "bo_order_id",
                "subject": "syncOrders"
            },
            "invoices": {
                "area": "frontend",
                "srcKey": "bo_invoice_id",
                "tgtKey": "fe_invoice_id",
                "subject": "syncInvoices"
            },
            "customers-bo": {
                "area": "backoffice",
                "srcKey": "fe_customer_id",
                "tgtKey": "bo_customer_id",
                "subject": "syncBOCustomers"
            },
            "customers-fe": {
                "area": "frontend",
                "srcKey": "bo_customer_id",
                "tgtKey": "fe_customer_id",
                "subject": "syncFECustomers"
            },
            "shipments": {
                "area": "frontend",
                "srcKey": "bo_shipment_id",
                "tgtKey": "fe_shipment_id",
                "subject": "syncShipments"
            },
            "purchaseorders": {
                "area": "frontend",
                "srcKey": "bo_po_num",
                "tgtKey": "fe_po_num",
                "subject": "syncPurchaseOrders"
            },
            "itemreceipts": {
                "area": "backoffice",
                "srcKey": "bo_po_num",
                "tgtKey": "bo_itemreceipt_id",
                "subject": "syncItemReceipts"
            },
            "products": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProducts"
            },
            "products-customoption": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-inventory": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-imagegallery": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-links": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-categories": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-pricelevels": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            },
            "products-variants": {
                "area": "frontend",
                "srcKey": "sku",
                "tgtKey": "fe_product_id",
                "subject": "syncProductsExtData"
            }
        }
        self.boTables = [k for k, v in self.tables.items() if v["area"] == "backoffice"]
        self.feTables = [k for k, v in self.tables.items() if v["area"] == "frontend"]

    @property
    def syncControl(self):
        return self._syncControl

    def getTask(self, table, id):
        task = {"ready": 0}
        response = dynamodb.Table(table).query(
            KeyConditionExpression=Key('id').eq(id),
            # Limit=1
        )
        if response['Count'] != 0:
            item = response["Items"][0]
            task["status"] = item["tx_status"]
            task["detail"] = {"note": item["tx_note"]}
            pk = self.tables[table]["tgtKey"]
            if pk in item.keys():
                task["detail"][pk] = item[pk]
            if task["status"] not in ["N", "P"]:
                task["ready"] = 1

        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(task, indent=4, cls=JSONEncoder))
        }

    def getCutDt(self, frontend, task):
        cutDt = os.environ["DEFAULTCUTDT"]
        offset = 0
        response = self.syncControl.query(
            IndexName="task_index",
            KeyConditionExpression=Key('task').eq(task),
            FilterExpression=Attr('frontend').eq(frontend) & Attr('sync_status').is_in(['Completed', 'Fail', 'Incompleted', 'Processing']),
            ProjectionExpression="id,cut_dt,#offset",
            ScanIndexForward=False,
            ExpressionAttributeNames={"#offset": "offset"}
        )
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = self.syncControl.query(
                IndexName="task_index",
                KeyConditionExpression=Key('task').eq(task),
                FilterExpression=Attr('frontend').eq(frontend) & Attr('sync_status').is_in(['Completed', 'Fail', 'Incompleted']),
                ProjectionExpression="id,cut_dt,#offset",
                ScanIndexForward=False,
                ExpressionAttributeNames={"#offset": "offset"},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
        if len(items) >= 1:
            lastItem = max(
                items,
                key=lambda item:(
                    datetime.strptime(item['cut_dt'], "%Y-%m-%d %H:%M:%S"),
                    int(item['offset'])
                )
            )
            id = lastItem['id']
            cutDt = lastItem['cut_dt']
            offset = int(lastItem['offset'])
            # Flsuh Sync Control Table by frontend and task.
            self.flushSyncControl(id, frontend, task)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps({
                        "cut_dt": cutDt,
                        "offset": offset
                    },
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def flushSyncControl(self, id, frontend, task):
        response = self.syncControl.query(
            IndexName="task_index",
            KeyConditionExpression=Key('task').eq(task),
            FilterExpression=Attr('id').ne(id) & \
                Attr('frontend').eq(frontend) & \
                Attr('start_dt').lt((datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')) & \
                Attr('sync_status').eq('Completed'),
            ProjectionExpression="id,frontend,task,sync_status",
        )

        data = response['Items']

        while 'LastEvaluatedKey' in response:
            response = self.syncControl.query(
                IndexName="task_index",
                KeyConditionExpression=Key('task').eq(task),
                FilterExpression=Attr('id').ne(id) & \
                    Attr('frontend').eq(frontend) & \
                    Attr('start_dt').lt((datetime.utcnow() - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')) & \
                    Attr('sync_status').eq('Completed'),
                ExclusiveStartKey=response['LastEvaluatedKey'],
                ProjectionExpression="id,frontend,task,sync_status"
            )
            data.extend(response['Items'])

        for item in data:
            response = self.syncControl.delete_item(Key={'id': item["id"]})

    def insertSyncTask(self, backoffice, frontend, task, table, syncTask):
        id = str(uuid.uuid1().int>>64)
        _syncTask = {
            'id': id,
            'store_code': syncTask['store_code'],
            'frontend': frontend,
            'backoffice': backoffice,
            'task': task,
            'table': table,
            'sync_status': 'Processing',
            'start_dt': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            'cut_dt': syncTask['cut_dt'],
            'offset': 0 if "offset" not in syncTask.keys() else syncTask["offset"],
            'sync_note': 'Process task(%s) for frontend(%s).' % (task, frontend),
            'entities': []
        }

        if len(syncTask['entities']) > 0:
            for entity in syncTask['entities']:
                _syncTask['entities'].append(entity)
            self.syncControl.put_item(Item=_syncTask)

            self.dispatchSyncTask(backoffice, frontend, table, id, syncTask['entities'])

        return {
            "statusCode": 200,
            "headers": {},
            "body": id
        }

    def updateSyncTask(self, id, entities):
        response = self.syncControl.get_item(
            Key={
                'id': id,
            }
        )
        _entities = response["Item"]["entities"]

        sync_status = 'Completed'
        while len(entities):
            entity = entities.pop(0)
            for _entity in _entities:
                if _entity["id"] == entity["id"]:
                    _entity["task_status"] = entity["task_status"]
                    _entity["task_detail"] = entity["task_detail"]
                    if _entity['task_status'] == 'F':
                        sync_status = 'Fail'
                    if _entity['task_status'] == '?':
                        sync_status = 'Incompleted'
                    break

        response = self.syncControl.update_item(
            Key={'id': id},
            UpdateExpression='SET sync_status = :val1, end_dt = :val2, entities = :val3',
            ExpressionAttributeValues={
                ':val1': sync_status,
                ':val2': datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                ':val3': _entities
            },
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(response, indent=4, cls=JSONEncoder))
        }

    def getSyncTask(self, id):
        response = self.syncControl.get_item(
            Key={
                'id': id,
            }
        )
        syncTask = response['Item']
        return {
            "statusCode": 200,
            "headers": {},
            "body": (json.dumps(syncTask, indent=4, cls=JSONEncoder))
        }

    def delSyncTask(self, id):
        self.syncControl.delete_item(
            Key={
                'id': id,
            }
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": "The syncTask(%s) is deleted." % id
        }

    def getSyncTasks(self):
        pass

    def dispatchSyncTask(self, backoffice, frontend, table, id, entities):
        queueName = "{0}_{1}_{2}_{3}".format(backoffice, frontend, table, id)[:75] + ".fifo"

        if table in self.boTables:
            maxTaskAgents = int(os.environ["BACKOFFICEMAXTASKAGENTS"])
            functionName = os.environ["BACKOFFICETASKARN"]
        elif table in self.feTables:
            maxTaskAgents = int(os.environ["FRONTENDMAXTASKAGENTS"])
            functionName = os.environ["FRONTENDTASKARN"]
        else:
            maxTaskAgents = 1

        try:
            taskQueue = sqs.create_queue(
                QueueName=queueName,
                Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"}
            )
            subject = self.tables[table]["subject"]
            for entity in entities:
                response = dynamodb.Table(table).get_item(
                    Key={
                        'id': entity["id"],
                    },
                    ProjectionExpression="frontend,{0},data_type,tx_status,tx_note".format(self.tables[table]["srcKey"])
                )
                item = response['Item']
                if item['tx_status'] == "N":
                    try:
                        taskQueue.send_message(
                            MessageBody=json.dumps(item, indent=4, cls=JSONEncoder),
                            MessageGroupId=id
                        )
                    except Exception as e:
                        log = traceback.format_exc()
                        logger.exception(log)
                        item['tx_status'] = "F"
                        item['tx_note'] = log
                        dynamodb.Table(table).put_item(item)

            while(maxTaskAgents):
                response = boto3.client('lambda').invoke(
                    FunctionName=functionName,
                    InvocationType='Event',
                    Payload=json.dumps(
                        {
                            "subject": subject,
                            "queueName": queueName
                        }
                    ),
                )
                maxTaskAgents -= 1
        except Exception as e:
            log = traceback.format_exc()
            logger.exception(log)
            raise
