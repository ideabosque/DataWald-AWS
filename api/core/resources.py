#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, os, traceback, sys

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

from models import ConfigDataModel, ProductMasterMetadataModel, ConnectionsModel
configDataModel = ConfigDataModel()
productMasterMetadataModel = ProductMasterMetadataModel()
connectionsModel = ConnectionsModel()

def handler(event, context):
    # TODO implement
    try:
        function = event["pathParameters"]["proxy"]
        if function == "config" and event["httpMethod"] == "POST":
            configEntity = json.loads(event["body"])
            return configDataModel.insertConfigEntity(configEntity)
        elif function == "config" and event["httpMethod"] == "GET":
            key = event["queryStringParameters"]["key"]
            return configDataModel.getConfigEntity(key)
        elif function == "config" and event["httpMethod"] == "PUT":
            key = event["queryStringParameters"]["key"]
            configEntity = json.loads(event["body"])
            return configDataModel.updateConfigEntity(key, configEntity)
        elif function == "config" and event["httpMethod"] == "DELETE":
            key = event["queryStringParameters"]["key"]
            return configDataModel.delConfigEntity(key)
        elif function == 'connections' and event["httpMethod"] == "POST":
            connection = json.loads(event["body"])
            return connectionsModel.insertConnection(connection)
        elif function == 'connections' and event["httpMethod"] == "GET":
            area = event["queryStringParameters"]["area"]
            id = event["queryStringParameters"]["id"]
            return connectionsModel.getConnection(area, id)
        elif function == 'connections' and event["httpMethod"] == "PUT":
            connection = json.loads(event["body"])
            area = event["queryStringParameters"]["area"]
            id = event["queryStringParameters"]["id"]
            return connectionsModel.updateConnection(area, id, connection)
        elif function == "connections" and event["httpMethod"] == "DELETE":
            area = event["queryStringParameters"]["area"]
            id = event["queryStringParameters"]["id"]
            return connectionsModel.delConnection(area, id)
        elif function == 'productmastermetadata' and event["httpMethod"] == "POST":
            metadataEntty = json.loads(event["body"])
            return productMasterMetadataModel.insertMetadataEntity(metadataEntty)
        elif function == 'productmastermetadata' and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            table = event["queryStringParameters"]["table"]
            return productMasterMetadataModel.getMetadata(frontend, table)
        elif function == 'productmastermetadata' and event["httpMethod"] == "PUT":
            metadataEntty = json.loads(event["body"])
            frontend = event["queryStringParameters"]["frontend"]
            column = event["queryStringParameters"]["column"]
            return productMasterMetadataModel.updateMetadataEntity(frontend, column, metadataEntty)
        elif function == "productmastermetadata" and event["httpMethod"] == "DELETE":
            frontend = event["queryStringParameters"]["frontend"]
            column = event["queryStringParameters"]["column"]
            return productMasterMetadataModel.delMetadataEntity(frontend, column)
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
