import boto3, json, traceback

class SQSConnector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger

    def connect(self):
        regionName = self.setting['REGIONNAME'] if self.setting is not None and 'REGIONNAME' in self.setting.keys() else None
        awsAccessKeyId = self.setting['AWSACCESSKEYID'] if self.setting is not None and 'AWSACCESSKEYID' in self.setting.keys() else None
        awsSecretAccessKey = self.setting['AWSSECRETACCESSKEY'] if self.setting is not None and 'AWSSECRETACCESSKEY' in self.setting.keys() else None

        if regionName is not None and awsAccessKeyId is not None and awsSecretAccessKey is not None:
            return boto3.client(
                'sqs',
                region_name=regionName,
                aws_access_key_id=awsAccessKeyId,
                aws_secret_access_key=awsSecretAccessKey
            )
        else:
            return boto3.client('sqs')

    @property
    def sqs(self):
        return self.connect()

    def getQueueUrl(self, queueName=None):
        response = self.sqs.list_queues(QueueNamePrefix=queueName)
        queueUrl = None
        if "QueueUrls" in response.keys():
            queueUrl = response["QueueUrls"][0]
        return queueUrl

    def getData(self, queueName=None):
        data = []
        queueUrl = self.getQueueUrl(queueName=queueName)
        if queueUrl is not None:
            while True:
                try:
                    response = self.sqs.receive_message(
                        QueueUrl=queueUrl,
                        MaxNumberOfMessages=10
                    )
                    messages = []
                    if "Messages" in response.keys():
                        messages = response["Messages"]
                    if len(messages) != 0:
                        for message in messages:
                            data.append(json.loads(message["Body"]))
                            self.sqs.delete_message(
                                QueueUrl=queueUrl,
                                ReceiptHandle=message["ReceiptHandle"]
                            )
                    else:
                        self.sqs.delete_queue(QueueUrl=queueUrl)
                        break
                except Exception as e:
                    log = traceback.format_exc()
                    self.logger.exception(log)
                    break
        return data
