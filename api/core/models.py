#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, uuid, decimal, os
from datetime import datetime, date
from decimal import Decimal

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

import boto3
from boto3.dynamodb.conditions import Key, Attr
dynamodb = boto3.resource('dynamodb')

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


class ConfigDataModel(object):

    def __init__(self):
        self._configData = dynamodb.Table('config_data')

    @property
    def configData(self):
        return self._configData

    def _decimalDecode(self, element):
        for k, v in element.items():
            if isinstance(v, float):
                element[k] = Decimal(str(v))
            elif isinstance(v, dict):
                self._decimalDecode(v)

    def insertConfigEntity(self, configEntity):
        self._decimalDecode(configEntity)
        self.configData.put_item(Item=configEntity)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConfigEntity(configEntity["key"]),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def _getConfigEntity(self, key):
        response = self.configData.get_item(
            Key={
                'key': key
            }
        )
        configEntity = response['Item']
        return configEntity

    def getConfigEntity(self, key):
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConfigEntity(key),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def updateConfigEntity(self, key, configEntity):
        self._decimalDecode(configEntity)
        self.configData.delete_item(
            Key={
                'key': key,
            }
        )
        self.configData.put_item(Item=configEntity)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConfigEntity(key),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def delConfigEntity(self, key):
        self.configData.delete_item(
            Key={
                'key': key,
            }
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": "The onfigEntity({0}) is deleted.".format(key)
        }


class ConnectionsModel(object):

    def __init__(self):
        self._connections = dynamodb.Table('connections')

    @property
    def connections(self):
        return self._connections

    def _decimalDecode(self, element):
        for k, v in element.items():
            if isinstance(v, float):
                element[k] = Decimal(str(v))
            elif isinstance(v, dict):
                self._decimalDecode(v)

    def insertConnection(self, connection):
        self._decimalDecode(connection)
        self.connections.put_item(Item=connection)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConnection(connection['area'], connection['id']),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def _getConnection(self, area, id):
        response = self.connections.get_item(
            Key={
                'area': area,
                'id': id
            }
        )
        connection = response['Item']
        return connection

    def getConnection(self, area, id):
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConnection(area, id),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def updateConnection(self, area, id, connection):
        self._decimalDecode(connection)
        response = self.connections.update_item(
            Key={
                'area': area,
                'id': id
            },
            UpdateExpression="set #agent=:val0, connector=:val1",
            ExpressionAttributeValues={
                ':val0': connection.get('agent', {}),
                ':val1': connection.get('connector', {})
            },
            ExpressionAttributeNames={"#agent": "agent"},
            ReturnValues="UPDATED_NEW"
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getConnection(area, id),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def delConnection(self, area, id):
        self.connections.delete_item(
            Key={
                'area': area,
                'id': id
            }
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": "{0}/{1} is deleted.".format(area, id)
        }


class ProductMasterMetadataModel(object):

    def __init__(self):
        self._productMasterMetadata = dynamodb.Table('product_master_metadata')

    @property
    def productMasterMetadata(self):
        return self._productMasterMetadata

    def _decimalDecode(self, element):
        for k, v in element.items():
            if isinstance(v, float):
                element[k] = Decimal(str(v))
            elif isinstance(v, dict):
                self._decimalDecode(v)

    def insertMetadataEntity(self, metadataEntty):
        self._decimalDecode(metadataEntty)
        self.productMasterMetadata.put_item(Item=metadataEntty)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getMetadataEntity(metadataEntty['frontend'], metadataEntty['column']),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def _getMetadataEntity(self, frontend, column):
        response = self.productMasterMetadata.get_item(
            Key={
                'frontend': frontend,
                'column': column
            }
        )
        metadataEntty = response['Item']
        return metadataEntty

    def _getMetadata(self, frontend, table):
        response = self.productMasterMetadata.query(
            IndexName="frontend_table_index",
            KeyConditionExpression=Key('frontend').eq(frontend) & Key('table').eq(table)
        )
        metadata = response['Items']
        return metadata

    def getMetadata(self, frontend, table):
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getMetadata(frontend, table),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def updateMetadataEntity(self, frontend, column, metadataEntty):
        self._decimalDecode(metadataEntty)
        self.productMasterMetadata.delete_item(
            Key={
                'frontend': frontend,
                'column': column
            }
        )
        self.productMasterMetadata.put_item(Item=metadataEntty)
        return {
            "statusCode": 200,
            "headers": {},
            "body": (
                json.dumps(
                    self._getMetadataEntity(frontend, column),
                    indent=4,
                    cls=JSONEncoder
                )
            )
        }

    def delMetadataEntity(self, frontend, column):
        self.productMasterMetadata.delete_item(
            Key={
                'frontend': frontend,
                'column': column
            }
        )
        return {
            "statusCode": 200,
            "headers": {},
            "body": "{0}/{1} is deleted.".format(frontend, column)
        }
