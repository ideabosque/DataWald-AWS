#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import json, traceback, boto3, os
from time import sleep

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))
lastRequestId = None

functs = {
    "updateSyncTask": {
        "fe": "updateSyncTask",
        "bo": "updateSyncTask"
    },
    "syncOrders": {
        "fe": "syncOrders",
        "bo": "insertOrders"
    },
    "reSyncOrders": {
        "fe": "reSyncOrders"
    },
    "syncBOCustomers": {
        "fe": "syncBOCustomers",
        "bo": "insertCustomers"
    },
    "reSyncBOCustomers": {
        "fe": "reSyncCustomers"
    },
    "syncItemReceipts": {
        "fe": "syncItemReceipts",
        "bo": "insertItemReceipts"
    },
    "reSyncItemReceipts": {
        "fe": "reSyncItemReceipts"
    },
    "syncInvoices": {
        "fe": "syncInvoices",
        "bo": "retrieveInvoices"
    },
    "reSyncInvoices": {
        "bo": "reSyncInvoices"
    },
    "syncFECustomers": {
        "fe": "syncFECustomers",
        "bo": "retrieveCustomers"
    },
    "reSyncFECustomers": {
        "bo": "reSyncCustomers"
    },
    "syncShipments": {
        "fe": "syncShipments",
        "bo": "retrieveShipments"
    },
    "reSyncShipments": {
        "bo": "reSyncShipments"
    },
    "syncPurchaseOrders": {
        "fe": "syncPurchaseOrders",
        "bo": "retrievePurchaseOrders"
    },
    "syncProducts": {
        "fe": "syncProducts",
        "bo": "retrieveProducts"
    },
    "reSyncProducts": {
        "bo": "reSyncProducts"
    },
    "syncProductsExtData": {
        "fe": "syncProductsExtData",
        "bo": "retrieveProductsExtData"
    }
}

from aws_dwconnector import DWConnector
DWSETTING = {
    "DWRESTENDPOINT": os.environ["DWRESTENDPOINT"],
    "DWAPIKEY": os.environ["DWAPIKEY"],
    # "DWRESTUSR": os.environ["DWRESTUSR"],
    # "DWRESTPASS": os.environ["DWRESTPASS"],
    # "DWUSERPOOLID": os.environ["DWUSERPOOLID"],
    # "DWCLIENTID": os.environ["DWCLIENTID"],
    # "DWSECRETKEY": os.environ["DWSECRETKEY"]
}
dwConnector = DWConnector(setting=DWSETTING, logger=logger)

def invoke(functionName, payload, invocationType="Event"):
    response = boto3.client('lambda').invoke(
        FunctionName=functionName,
        InvocationType=invocationType,
        Payload=json.dumps(payload),
    )
    if "FunctionError" in response.keys():
        log = json.loads(response['Payload'].read())
        logger.exception(log)
        raise Exception(log)
    if invocationType == "RequestResponse":
        return json.loads(response['Payload'].read())

def handler(event, context):
    # TODO implement
    global lastRequestId
    if lastRequestId == context.aws_request_id:
        return # abort
    else:
        lastRequestId = context.aws_request_id # keep request id for next invokation

    app = event["app"].split('.')
    funct = event["funct"]
    params = json.loads(event["params"])
    logger.info(app)
    logger.info(funct)
    logger.info(params)

    payload = {
        "app": event["app"],
        "hasRawData": "No",
        "DWSETTING": json.dumps(DWSETTING)
    }
    try:
        connection = dwConnector.getConnection(app[0], app[1].upper())
        functionName = connection["agent"]["functionName"]
        if app[0] == "fe":
            payload["funct"] = functs[funct]["fe"]
            if funct == "updateSyncTask":
                payload["params"] = json.dumps({"id": params["id"]})
                invoke(functionName, payload)
            elif funct in ["syncOrders", "syncItemReceipts", "syncBOCustomers"]:
                _params = {"storeCode": "0"}
                _params["backoffice"] = params.pop("backoffice")
                if len(params.keys()) > 0:
                    payload["hasRawData"] = "Yes"
                    _params = {**_params, **params}
                payload["params"] = json.dumps(_params)
                invoke(functionName, payload)
                # syncTask = invoke(functionName, payload, invocationType="RequestResponse")
                # logger.info(syncTask)
                # return syncTask
            elif funct in ["reSyncOrders", "reSyncItemReceipts", "reSyncBOCustomers"]:
                payload["params"] = json.dumps({"id": params["id"]})
                syncTask = invoke(functionName, payload, invocationType="RequestResponse")
                logger.info(syncTask)
                return syncTask
            elif funct in ["syncInvoices", "syncPurchaseOrders", "syncProducts", "syncProductsExtData", "syncShipments", "syncFECustomers"]:
                payload["params"] = json.dumps({"data": params["data"]})
                invoke(functionName, payload)
        elif app[0] == "bo":
            payload["funct"] = functs[funct]["bo"]
            if funct == 'updateSyncTask':
                payload["params"] = json.dumps({"id": params["id"]})
                invoke(functionName, payload)
            elif funct in ['syncOrders', 'syncItemReceipts', 'syncBOCustomers']:
                payload["params"] = json.dumps({"data": params["data"]})
                invoke(functionName, payload)
            elif funct in ["reSyncInvoices", "reSyncProducts", "reSyncShipments", "reSyncFECustomers"]:
                payload["params"] = json.dumps({"id": params["id"]})
                syncTask = invoke(functionName, payload, invocationType="RequestResponse")
                logger.info(syncTask)
                return syncTask
            elif funct in ['syncInvoices', 'syncPurchaseOrders', 'syncProducts', 'syncProductsExtData', 'syncShipments', 'syncFECustomers']:
                invocationType = params.pop('invocationType', 'event')
                _params = {"storeCode": "0"}
                _params["frontend"] = params.pop('frontend')

                table = params.pop('table', None)
                if table is not None:
                    _params["table"] = table.replace("+"," ")

                dataType = params.pop('dataType', None)
                if dataType is not None:
                    _params["dataType"] = dataType

                limit = params.pop('limit', None)
                if limit is not None:
                    _params["limit"] = limit

                if 'key' in params:
                    params['key'] = params['key'].replace("+"," ")
                if len(params.keys()) > 0:
                    payload["hasRawData"] = "Yes"
                    _params = {**_params, **params}
                payload["params"] = json.dumps(_params)

                if invocationType == "event":
                    invoke(functionName, payload)
                else:
                    syncTask = invoke(functionName, payload, invocationType="RequestResponse")
                    logger.info(syncTask)
                    return syncTask
    except Exception as e:
        log = traceback.format_exc()
        logger.exception(log)
        boto3.client("sns").publish(
            TopicArn=os.environ["SNSTOPICARN"],
            Subject=context.invoked_function_arn,
            MessageStructure="json",
            Message=json.dumps(
                {"default": json.dumps({
                        "app": app,
                        "funct": funct,
                        "params": params,
                        "log": log
                    })
                }
            )
        )
