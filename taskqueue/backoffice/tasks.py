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

def getQueueAttributes(queueUrl=None):
    response = boto3.client('sqs').get_queue_attributes(
        QueueUrl=queueUrl,
        AttributeNames=['All']
    )
    attributes = response["Attributes"]
    totalMessages = int(attributes["ApproximateNumberOfMessages"]) + int(attributes["ApproximateNumberOfMessagesNotVisible"]) + int(attributes["ApproximateNumberOfMessagesDelayed"])
    attributes["TotalMessages"] = totalMessages
    return attributes

def syncData(backoffice, queueName, subject):
    data = []
    totalMessages = 0
    queueUrl = None

    try:
        response = boto3.client('sqs').list_queues(QueueNamePrefix=queueName)
        if "QueueUrls" in response.keys():
            queueUrl = response["QueueUrls"][0]

        if queueUrl is not None:
            totalMessages = getQueueAttributes(queueUrl=queueUrl)["TotalMessages"]
            if totalMessages != 0:
                response = boto3.client('sqs').receive_message(
                    QueueUrl=queueUrl,
                    MaxNumberOfMessages=int(os.environ["SQSMAXMSG"]),
                    VisibilityTimeout=600
                )
                for message in response['Messages']:
                    data.append(json.loads(message['Body']))
                    boto3.client('sqs').delete_message(
                        QueueUrl=queueUrl,
                        ReceiptHandle=message['ReceiptHandle']
                    )
                    totalMessages = totalMessages - 1
            if totalMessages == 0:
                boto3.client('sqs').delete_queue(QueueUrl=queueUrl)
    except Exception as e:
        log = traceback.format_exc()
        logger.exception(log)
        raise

    if len(data) != 0:
        payload = {
            "app": "{0}.{1}".format("bo", backoffice),
            "funct": subject,
            "params": json.dumps({"data": data})
        }
        invoke(os.environ["CORETASKARN"], payload)
    return (queueUrl, len(data), totalMessages)


def handler(event, context):
    # TODO implement
    global lastRequestId
    if lastRequestId == context.aws_request_id:
        return # abort
    else:
        lastRequestId = context.aws_request_id # keep request id for next invokation

    try:
        if 'Records' in event.keys() and event['Records'][0]['eventSource'] == "aws:s3":
            record = event['Records'][0]
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            keys = key.split('/')
            if keys[0] == "product":
                payload = {
                    "app": "{0}.{1}".format("bo", "s3"),
                    "funct": "syncProducts",
                    "params": json.dumps(
                        {
                            "bucket": bucket,
                            "key": key,
                            "newLine": "\n",
                            "frontend": keys[1],
                            "table": keys[2]
                        }
                    )
                }
                invoke(os.environ["CORETASKARN"], payload)
            elif keys[0] in ["customoption",'imagegallery','links','categories','inventory','pricelevels']:
                payload = {
                    "app": "{0}.{1}".format("bo", "s3"),
                    "funct": "syncProductsExtData",
                    "params": json.dumps(
                        {
                            "bucket": bucket,
                            "key": key,
                            "newLine": "\n",
                            "frontend": keys[1],
                            "dataType" : keys[0]
                        }
                    )
                }
                invoke(os.environ["CORETASKARN"], payload)

        else:
            subject = event['subject']
            if subject in ["syncOrders", "syncItemReceipts"]:
                queueName = event['queueName']
                i = (queueName.replace(".fifo", "", 1)).split('_')
                backoffice = i[0].upper()
                feApp = i[1].upper()
                table = i[2]
                id = i[3]
                try:
                    (queueUrl, processedMessages, totalMessages) = syncData(backoffice, queueName, subject)
                except Exception as e:
                    log = traceback.format_exc()
                    logger.exception(log)
                    sleep(15)
                    invoke(
                        context.invoked_function_arn,
                        {
                            "subject": subject,
                            "queueName": queueName
                        }
                    )
                    return

                logger.info({"queueUrl": queueUrl, "processedMessages": processedMessages, "totalMessages": totalMessages})
                if queueUrl is not None:
                    if totalMessages == 0:
                        sleep(15)
                        invoke(
                            os.environ["CORETASKARN"],
                            {
                                "app": "{0}.{1}".format("fe", feApp),
                                "funct": "updateSyncTask",
                                "params": json.dumps({"id": id})
                            }
                        )
                    else:
                        sleep(5)
                        invoke(
                            context.invoked_function_arn,
                            {
                                "subject": subject,
                                "queueName": queueName
                            }
                        )
            elif subject in ["syncInvoices", "syncPurchaseOrders", "syncFECustomers"]:
                backoffice = event["backoffice"]
                feApp = event["feApp"]
                payload = {
                    "app": "{0}.{1}".format("bo", backoffice),
                    "funct": subject,
                    "params": json.dumps(
                        {
                            "frontend": feApp
                        }
                    )
                }
                invoke(os.environ["CORETASKARN"], payload)
            elif subject == "syncProducts":
                backoffice = event["backoffice"]
                feApp = event["feApp"]
                tables = event["tables"].split("|")
                for table in tables:
                    params = {"frontend": feApp, "table": table}
                    if "limit" in event.keys():
                        params = dict(params, **{"limit": event["limit"]})
                    payload = {
                        "app": "{0}.{1}".format("bo", backoffice),
                        "funct": "syncProducts",
                        "params": json.dumps(params)
                    }
                    invoke(os.environ["CORETASKARN"], payload)
            elif subject == "syncProductsExtData":
                backoffice = event["backoffice"]
                feApp = event["feApp"]
                dataType = event["dataType"]
                params = {"frontend": feApp, "dataType": dataType}
                if "limit" in event.keys():
                    params = dict(params, **{"limit": event["limit"]})
                payload = {
                    "app": "{0}.{1}".format("bo", backoffice),
                    "funct": "syncProductsExtData",
                    "params": json.dumps(params)
                }
                invoke(os.environ["CORETASKARN"], payload)


    except Exception as e:
        log = traceback.format_exc()
        logger.exception(log)
        boto3.client("sns").publish(
            TopicArn=os.environ["SNSTOPICARN"],
            Subject=context.invoked_function_arn,
            MessageStructure="json",
            Message= json.dumps({"default": log})
        )
