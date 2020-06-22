#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import sys
sys.path.append('/opt')
import json, traceback, boto3, os
from time import sleep

import logging
logger = logging.getLogger()
logger.setLevel(eval(os.environ["LOGGINGLEVEL"]))
lastRequestId = None

from aws_dwconnector import DWConnector

Connector = getattr(__import__(os.environ["CONNECTORMODULE"]), os.environ["CONNECTORCLASS"])

def handler(event, context):
    # TODO implement
    global lastRequestId
    if lastRequestId == context.aws_request_id:
        return # abort
    else:
        lastRequestId = context.aws_request_id # keep request id for next invokation

    app = event["app"].split('.')
    funct = event["funct"]
    hasRawData = event.get("hasRawData", "No")
    params = json.loads(event["params"])
    DWSETTING = json.loads(event["DWSETTING"])

    dwConnector = DWConnector(setting=DWSETTING, logger=logger)
    connection = dwConnector.getConnection(app[0], app[1].upper())
    Agent = getattr(
        __import__(connection["agent"]["AGENTMODULE"]),
        connection["agent"]["AGENTCLASS"]
    )
    connector = Connector(setting=connection["connector"]["setting"], logger=logger)
    if app[0] == "fe":
        agent = Agent(
            setting=connection["agent"]["setting"],
            logger=logger,
            feApp=app[1].upper(),
            dataWald=dwConnector,
            feConn=connector
        )
    elif app[0] == "bo":
        agent = Agent(
            setting=connection["agent"]["setting"],
            logger=logger,
            boApp=app[1].upper(),
            dataWald=dwConnector,
            boConn=connector
        )
    else:
        log = "There is no support for {}.  Please verify the setting.".format(event["app"])
        logger.exception(log)
        raise Exception(log)

    if hasRawData == "Yes":
        agent.setRawData(**params)
    return getattr(agent, funct)(**params)
