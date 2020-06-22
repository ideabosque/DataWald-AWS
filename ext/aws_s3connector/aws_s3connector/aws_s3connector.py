import boto3, csv, xmltodict, json, uuid, copy, traceback
from io import StringIO
from dicttoxml import dicttoxml
from datetime import datetime, timedelta
from xml.dom.minidom import parseString
from time import sleep

class S3Connector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger

    def connect(self):
        awsAccessKeyId = self.setting['AWSACCESSKEYID'] if self.setting is not None and 'AWSACCESSKEYID' in self.setting.keys() else None
        awsSecretAccessKey = self.setting['AWSSECRETACCESSKEY'] if self.setting is not None and 'AWSSECRETACCESSKEY' in self.setting.keys() else None

        if awsAccessKeyId is not None and awsSecretAccessKey is not None:
            return boto3.client(
                's3',
                aws_access_key_id=awsAccessKeyId,
                aws_secret_access_key=awsSecretAccessKey
            )
        else:
            return boto3.client('s3')

    @property
    def s3(self):
        return self.connect()

    def _removeEmptyValue(self,row):
        newRow = {}
        for key, value in row.items():
            key = key.strip()
            value = value.strip()
            if key !='' and value != '':
                newRow[key] = value
        return newRow

    def getRows(self, bucket, key, newLine="\r"):
        coding = self.setting['FILEENCODING'] if self.setting is not None and 'FILEENCODING' in self.setting.keys() else 'utf-8'
        #self.logger.info(coding)
        #self.logger.info(self.s3.get_object(Bucket=bucket, Key=key)["Body"])
        content = self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        content = content.decode(coding).encode('utf-8')
        lines = content.split(newLine)
        rows = []
        for row in csv.DictReader(lines):
            #self.logger.info(row)
            row = self._removeEmptyValue(row)
            #self.logger.info(row)
            if row:
                rows.append(row)

        if key.find('.csv') != -1:
            self.s3.copy_object(
                CopySource={'Bucket': bucket, 'Key': key},
                Bucket=bucket,
                Key="archive/{} ({})".format(key, datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
            )
            self.s3.delete_object(Bucket=bucket, Key=key)

        if len(rows) <= 200:
            return rows
        else:
            suffix = 0
            rowMaxAmountKeys = max(rows, key=lambda row:len(row.keys()))
            keys = rowMaxAmountKeys.keys()
            for i in range(0, len(rows), 200):
                output = StringIO()
                writer = csv.DictWriter(output, keys, delimiter=',')
                writer.writeheader()
                writer.writerows(rows[i:i+200])
                self.s3.put_object(
                    Bucket=bucket,
                    Key=key.replace(".csv", ".{0}.csv".format(suffix)),
                    Body=output.getvalue()
                )
                suffix = suffix + 1
                sleep(5)
            return []

    def _dict2CSV(self, data):
        rows = []
        _lists = list(filter(lambda v: isinstance(v, list), data.values()))
        while len(_lists):
            _list = _lists.pop()
            if len(rows) == 0:
                rows = [
                    dict(
                        {k: v for k, v in data.items() if not isinstance(v, (dict, list))},
                        **{k: v for k, v in item.items()}
                    ) for item in _list
                ]

                # load attributes from dict.
                for value in list(filter(lambda v: isinstance(v, dict), data.values())):
                    rows = [
                        dict(
                            row, 
                            **{k:v for k,v in value.items()}
                        ) for row in rows
                    ]
            else:
                _rows = []
                for row in rows:
                    _rows.extend( 
                        [
                            dict(
                                row,
                                **{k: v for k, v in item.items()}
                            ) for item in _list
                        ]
                    )
                rows = _rows
        return rows

    def putObj(self, key, doc, indent=4):
        bucket = self.setting["EXPORTBUCKET"]
        exportType = self.setting["EXPORTTYPE"]
        output = StringIO()
        if exportType == "xml":
            obj = dicttoxml(doc["data"], attr_type=False)
            obj = parseString(obj)
            output.write(obj.toprettyxml())
        elif exportType == "json" and indent is not None:
            output.write(json.dumps(doc["data"] ,indent=indent))
        elif exportType == "json" and indent is None:
            output.write(json.dumps(doc["data"]))
        elif exportType == "csv":
            rows = self._dict2CSV(doc["data"])
            writer = csv.DictWriter(output, rows[0].keys(), delimiter=',')
            writer.writeheader()
            writer.writerows(rows)
        self.s3.put_object(
            Bucket=bucket,
            Key="{}.{}".format(key,exportType),
            Body=output.getvalue()
        )

    def getMatchingS3Keys(self, bucket, prefix='', suffix=''):
        """
        Generate the keys in an S3 bucket.

        :param bucket: Name of the S3 bucket.
        :param prefix: Only fetch keys that start with this prefix (optional).
        :param suffix: Only fetch keys that end with this suffix (optional).
        """
        kwargs = {'Bucket': bucket}

        # If the prefix is a single string (not a tuple of strings), we can
        # do the filtering directly in the S3 API.
        if isinstance(prefix, str):
            kwargs['Prefix'] = prefix

        while True:

            # The S3 API response is a large blob of metadata.
            # 'Contents' contains information about the listed objects.
            resp = self.s3.list_objects_v2(**kwargs)
            for obj in resp['Contents']:
                key = obj['Key']
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            # The S3 API is paginated, returning up to 1000 keys at a time.
            # Pass the continuation token into the next response, until we
            # reach the final page (when this field is missing).
            try:
                kwargs['ContinuationToken'] = resp['NextContinuationToken']
            except KeyError:
                break

    def getTotalObjs(self, folder, cutDt):
        bucket = self.setting["IMPORTBUCKET"]
        prefix = "{}/".format(folder)
        suffix = ".{}".format(self.setting["IMPORTTYPE"])
        objs = self.getMatchingS3Keys(bucket, prefix=prefix, suffix=suffix)

        cutDt = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S")
        objs = list(filter(lambda t: (t["LastModified"].replace(tzinfo=None)>cutDt), objs))
        return len(objs)

    def getObjs(self, folder, offset=0, limit=5, cutDt=None):
        coding = self.setting['FILEENCODING'] if self.setting is not None and 'FILEENCODING' in self.setting.keys() else 'utf-8'
        bucket = self.setting["IMPORTBUCKET"]
        prefix = "{}/".format(folder)
        suffix = ".{}".format(self.setting["IMPORTTYPE"])
        _objs = self.getMatchingS3Keys(bucket, prefix=prefix, suffix=suffix)

        if cutDt is not None:
            cutDt = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S")
            _objs = list(filter(lambda t: (t["LastModified"].replace(tzinfo=None)>cutDt), _objs))

        getLastModified = lambda _obj: int(_obj['LastModified'].strftime('%s'))
        _objs = [_obj for _obj in sorted(_objs, key=getLastModified)][offset:(offset+limit)]

        objs = []
        for _obj in _objs:
            _key = _obj['Key']
            _lastModified = _obj['LastModified'].strftime("%Y-%m-%d %H:%M:%S")
            _key = _key.replace(prefix, "", 1)
            _key = _key.replace(suffix, "", 1)
            content = self.s3.get_object(Bucket=bucket, Key=_obj['Key'])["Body"].read()
            content = content.decode(coding).encode('utf-8')
            obj = None
            if self.setting["IMPORTTYPE"] == "xml":
                obj = xmltodict.parse(content, force_list={"item": True})["root"]
                obj["key"] = _key
                obj["lastModified"] = _lastModified
            elif self.setting["IMPORTTYPE"] == "json":
                obj = json.loads(content)
                obj["key"] = _key
                obj["lastModified"] = _lastModified
            objs.append(obj)

            self.s3.copy_object(
                CopySource={'Bucket': bucket, 'Key': _obj['Key']},
                Bucket=bucket,
                # Key="archive/{} ({})".format(_obj['Key'], datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                Key="archive/{0}/{1}/{2}/{3}/{4}".format(
                    _obj['Key'].split("/")[0],
                    datetime.utcnow().year,
                    datetime.utcnow().month,
                    datetime.utcnow().day,
                    _obj['Key'].split("/")[1]
                )
            )
            self.s3.delete_object(Bucket=bucket, Key=_obj['Key'])

        return objs

    def insertOrder(self, frontend, feOrderId, order):
        # order.pop("tx_dt")
        order.pop("tx_status")
        order.pop("tx_note")
        feOrderDate = datetime.strptime(order["fe_order_date"], "%Y-%m-%d %H:%M:%S")
        key = "salesorder/{frontend}/{yyyy}/{mm}/{dd}/{fe_order_id}".format(
            frontend=frontend,
            yyyy=feOrderDate.year,
            mm=feOrderDate.month,
            dd=feOrderDate.day,
            fe_order_id=feOrderId
        ) # <- develop the rule for the key
        self.putObj(key, {"data": order}, indent=None)
        return uuid.uuid1().int>>64

    def insertOrders(self, orders):
        for order in orders:
            try:
                order["bo_order_id"] = self.insertOrder(order["frontend"], order["fe_order_id"], copy.deepcopy(order))
                order["tx_status"] = 'S'
            except Exception as e:
                log = traceback.format_exc()
                order["bo_order_id"] = "####"
                order["tx_status"] = 'F'
                order["tx_note"] = log
                self.logger.exception('Failed to create order: {0} with error: {1}'.format(order, e))
        return orders