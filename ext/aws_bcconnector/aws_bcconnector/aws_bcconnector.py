import requests, base64, json, time, traceback, math
from tenacity import retry, wait_exponential, stop_after_attempt
from datetime import datetime, timedelta, date
from decimal import Decimal

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

class BCConnector(object):

    def __init__(self, setting=None, logger=None):
        self.setting = setting
        self.logger = logger
        self.endpoint = 'https://api.bigcommerce.com/stores'

    def _jsonDumps(self, data):
        return json.dumps(data, indent=4, cls=JSONEncoder, ensure_ascii=False)

    @property
    def headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Auth-Client": self.setting["CLIENTID"],
            "X-Auth-Token": self.setting["ACCESSTOKEN"]
        }

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def getMethod(self, storeHash, uri, filter=None, version='v2'):
        requestUrl = "{0}/{1}/{2}".format(self.endpoint, storeHash, version) + uri
        # self.logger.info(requestUrl)
        response = requests.get(requestUrl, headers=self.headers, params=filter)
        self.logger.info(response.status_code)
        if response.status_code == 200:
            return json.loads(response.content)
        if response.status_code == 204: # can't find customer email (in CompanyModel isEmailTaken)
            return False
        else:
            self.logger.error(response.content)
            raise Exception(response.content)

    def getPages(self, storeHash, uri, limit, filter=None, version='v2'):
        if version == 'v2':
            count = self.getMethod(storeHash, uri, filter=filter, version=version)['count']
            pages = int(math.ceil(float(count) / float(limit)))
            return pages
        elif version == 'v3':
            if filter is None:
                filter = {'page': 1, 'limit': limit}
            else:
                filter['page'] = 1
                filter['limit'] = limit
            pagination = self.getMethod(storeHash, uri, filter=filter, version=version)['meta']['pagination']
            return pagination['total_pages']

    def getOrders(self, minDateCreated, maxDateCreated=None, limit=250):
        try:
            if maxDateCreated == None:
                deadline = datetime.strptime(minDateCreated, "%Y-%m-%d %H:%M:%S") + timedelta(days=int(self.setting["DEADLINEDAYS"]))
                maxDateCreated = deadline.strftime("%Y-%m-%d %H:%M:%S")
            uri = "/orders"
            pages = self.getPages(
                self.setting["STOREHASH"],
                "/orders/count",
                limit,
                filter={"min_date_created": minDateCreated, "max_date_created": maxDateCreated}
            )
            filter = {
                'limit': limit,
                'min_date_created': minDateCreated,
                'max_date_created': maxDateCreated
            }
            orders = []
            for i in range(0, pages):
                filter['page'] = i+1
                orders.extend(
                    self.getMethod(self.setting["STOREHASH"], uri, filter=filter)
                )
            return orders
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def getShippingAddresses(self, orderId):
        try:
            uri = "/orders/{0}/shippingaddresses".format(orderId)
            shippingAddresses = self.getMethod(self.setting["STOREHASH"], uri)
            return shippingAddresses
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

    def getOrderProducts(self, orderId):
        try:
            uri = "/orders/{0}/products".format(orderId)
            orderProducts = self.getMethod(self.setting["STOREHASH"], uri)
            return orderProducts
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
