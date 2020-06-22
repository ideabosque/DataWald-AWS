#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'bibow'

import sys
sys.path.append('/opt')
import json, traceback, boto3, os
from time import sleep
from sshtunnel import SSHTunnelForwarder

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

    try:
        setting = connection["connector"]["setting"]
        with SSHTunnelForwarder(
            (setting['SSHSERVER'], setting['SSHSERVERPORT']),
            ssh_username=setting['SSHUSERNAME'],
            ssh_pkey=setting.get('SSHPKEY', None),
            ssh_password=setting.get('SSHPASSWORD', None),
            remote_bind_address=(setting['REMOTEBINDSERVER'], setting['REMOTEBINDSERVERPORT']),
            local_bind_address=(setting['LOCALBINDSERVER'], setting['LOCALBINDSERVERPORT'])
        ) as server:
            try:
                if hasRawData == "Yes":
                    agent.setRawData(**params)
                res = getattr(agent, funct)(**params)
                del agent
                return res
            except Exception:
                del agent
                log = traceback.format_exc()
                logger.exception(log)
            server.stop()
            server.close()
    except Exception as e:
        log = 'Failed to connect ssh server with error: %s' % str(e)
        logger.exception(log)
        raise
