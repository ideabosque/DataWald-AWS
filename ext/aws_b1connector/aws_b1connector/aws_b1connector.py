import requests, base64, json

class B1Connector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger

    def getAccessToken(self, username, password):
        data = {
            "username": username,
            "password": password
        }
        requestPath = '/auth'
        requestUrl = self.setting['B1RESTENDPOINT'] + requestPath
        response = requests.post(
                                    requestUrl,
                                    headers={"Content-Type": "application/json"},
                                    data=json.dumps(data, ensure_ascii=False),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 200:
            accessToken = json.loads(response.content)["access_token"]
            return accessToken
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def connect(self):
        accessToken = self.getAccessToken(self.setting['B1RESTUSR'], self.setting['B1RESTPASS'])
        return {
            "Authorization": "JWT %s" % accessToken,
            "Content-Type": "application/json"
        }

    def getInfo(self):
        requestPath = '/v1/info'
        requestUrl = self.setting['B1RESTENDPOINT'] + requestPath
        response = requests.get(requestUrl, headers=self.headers)
        if response.status_code == 201:
            info = json.loads(response.content)
            return info
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getBoOrderId(self, id, frontendIdUdf=None):
        data = {
                "columns": ["DocEntry"],
                "params": {
                    (frontendIdUdf if frontendIdUdf is not None else 'NumAtCard'): {
                        'value': id
                    }
                }
            }
        requestPath = '/v1/orders/fetch?num=1'
        requestUrl = self.setting['B1RESTENDPOINT'] + requestPath
        response = requests.put(
                                    requestUrl,
                                    headers=self.headers,
                                    data=json.dumps(data, ensure_ascii=False),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 201:
            data = json.loads(response.content)
            boOrderid = None if len(data) == 0 else data[0]['DocEntry']
            return boOrderid
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def insertOrders(self, orders):
        requestPath = '/v1/orders/insert'
        requestUrl = self.setting['B1RESTENDPOINT'] + requestPath
        response = requests.post(
                                    requestUrl,
                                    headers=self.headers,
                                    data=json.dumps(orders, ensure_ascii=False),
                                    timeout=60,
                                    verify=True
                                )
        if response.status_code == 201:
            boOrders = json.loads(response.content)
            return boOrders
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    @property
    def headers(self):
        return self.connect()
