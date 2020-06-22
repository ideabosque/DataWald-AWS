#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, os, traceback, sys

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))

from models import SyncControlModel
syncControlModel = SyncControlModel()

def handler(event, context):
    # TODO implement
    try:
        function = event["pathParameters"]["proxy"]
        if function == "task" and event["httpMethod"] == "GET":
            table = event["queryStringParameters"]["table"]
            id = event["queryStringParameters"]["id"]
            return syncControlModel.getTask(table, id)
        elif function == "cutdt" and event["httpMethod"] == "GET":
            frontend = event["queryStringParameters"]["frontend"]
            task = event["queryStringParameters"]["task"]
            return syncControlModel.getCutDt(frontend, task)
        elif function == "synccontrol" and event["httpMethod"] == "PUT":
            backoffice = event["queryStringParameters"]["backoffice"]
            frontend = event["queryStringParameters"]["frontend"]
            task = event["queryStringParameters"]["task"]
            table = event["queryStringParameters"]["table"]
            syncTask = json.loads(event["body"])
            return syncControlModel.insertSyncTask(backoffice, frontend, task, table, syncTask)
        elif function == "synctask" and event["httpMethod"] == "PUT":
            id = event["queryStringParameters"]["id"]
            entities = json.loads(event["body"])
            return syncControlModel.updateSyncTask(id, entities)
        elif function == "synctask" and event["httpMethod"] == "GET":
            id = event["queryStringParameters"]["id"]
            return syncControlModel.getSyncTask(id)
        elif function == "synctask" and event["httpMethod"] == "DELETE":
            id = event["queryStringParameters"]["id"]
            return syncControlModel.delSyncTask(id)
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
