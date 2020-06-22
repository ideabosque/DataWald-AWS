from zeep import Client
from zeep.transports import Transport
from zeep import xsd
from datetime import datetime, timedelta
from pytz import timezone
from time import sleep, time
from tenacity import retry, wait_exponential, stop_after_attempt
import traceback, base64, hmac, hashlib, random

class Adaptor(object):
    """Adaptor connects with SuiteTask SOAP API.
    """
    version = '2020_1_0'
    wsdlUrlTmpl = 'https://{account_id}.suitetalk.api.netsuite.com/wsdl/v{underscored_version}/netsuite.wsdl'

    def __init__(self, setting, logger):
        self._setting = setting
        self.logger = logger
        self._client = Client(
            self.wsdlUrlTmpl.format(
                underscored_version=self._setting.get('VERSION', self.version),
                account_id=self._setting['ACCOUNT'].lower().replace('_', '-')
            ),
            transport=Transport(timeout=1000)
        )

    def _generateTimestamp(self):
        return str(int(time()))

    def _generateNonce(self, length=20):
        """Generate pseudorandom number
        """
        return ''.join([str(random.randint(0, 9)) for i in range(length)])

    def _getSignatureMessage(self, nonce, timestamp):
        return '&'.join(
            (
                self._setting['ACCOUNT'],
                self._setting['CONSUMER_KEY'],
                self._setting['TOKEN_ID'],
                nonce,
                timestamp,
            )
        )

    def _getSignatureKey(self):
        return '&'.join((self._setting['CONSUMER_SECRET'], self._setting['TOKEN_SECRET']))

    def _getSignatureValue(self, nonce, timestamp):
        key = self._getSignatureKey()
        message = self._getSignatureMessage(nonce, timestamp)
        hashed = hmac.new(
            key=key.encode('utf-8'),
            msg=message.encode('utf-8'),
            digestmod=hashlib.sha256
        ).digest()
        return base64.b64encode(hashed).decode()


    @property
    def client(self):
        return self._client

    @property
    def tokenPassport(self):
        TokenPassport = self.getDataType("ns0:TokenPassport")
        TokenPassportSignature = self.getDataType("ns0:TokenPassportSignature")

        nonce = self._generateNonce()
        timestamp = self._generateTimestamp()
        tokenPassportSignature = TokenPassportSignature(
            self._getSignatureValue(nonce, timestamp),
            algorithm='HMAC-SHA256'
        )

        return TokenPassport(
            account=self._setting['ACCOUNT'],
            consumerKey=self._setting['CONSUMER_KEY'],
            token=self._setting['TOKEN_ID'],
            nonce=nonce,
            timestamp=timestamp,
            signature=tokenPassportSignature
        )

    @property
    def applicationInfo(self):
        ApplicationInfo = self.getDataType("ns4:ApplicationInfo")
        applicationInfo = ApplicationInfo(applicationId='')
        return applicationInfo

    def getDataType(self, dataType):
        return self.client.get_type(dataType)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def get(self, baseRef=None):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.get(baseRef=baseRef, _soapheaders=soapheaders)
        if response["body"]["readResponse"]["status"]["isSuccess"] == True:
            return response["body"]["readResponse"]["record"]
        else:
            statusDetail = response["body"]["readResponse"]["status"]["statusDetail"]
            raise Exception(statusDetail)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def delete(self, baseRef=None):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.delete(baseRef=baseRef, _soapheaders=soapheaders)
        if response["body"]["writeResponse"]["status"]["isSuccess"] == True:
            return True
        else:
            statusDetail = response["body"]["writeResponse"]["status"]["statusDetail"]
            raise Exception(statusDetail)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def add(self, record=None):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.add(record=record, _soapheaders=soapheaders)
        if response["body"]["writeResponse"]["status"]["isSuccess"] == True:
            record = response["body"]["writeResponse"]["baseRef"]
            return record
        else:
            ## Additonal check if the record is inserted or not.
            if hasattr(response["body"]["writeResponse"], 'baseRef'):
                record = response["body"]["writeResponse"]["baseRef"]
                if record is not None:
                    return record
            statusDetail = response["body"]["writeResponse"]["status"]["statusDetail"]
            self.logger.error(statusDetail)
            _statusDetail = [status for status in statusDetail \
                if status["code"] not in ["DUP_ITEM"]]
            if len(_statusDetail) > 0:
                raise Exception(_statusDetail)
            else:
                return {"internalId": "DUP_ITEM"}

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def update(self, record=None):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.update(record=record, _soapheaders=soapheaders)
        if response["body"]["writeResponse"]["status"]["isSuccess"] == True:
            record = response["body"]["writeResponse"]["baseRef"]
            return record
        else:
            statusDetail = response["body"]["writeResponse"]["status"]["statusDetail"]
            self.logger.error(statusDetail)
            raise Exception(statusDetail)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def search(self, searchRecord=None, searchPreferences=None, advance=False):
        limitPages = self._setting.get("LIMIT_PAGES", 3)
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        if searchPreferences is not None:
            soapheaders["searchPreferences"] = searchPreferences
        response = self.service.search(searchRecord=searchRecord, _soapheaders=soapheaders)
        isSuccess = response["body"]["searchResult"]["status"]["isSuccess"]
        totalRecords = response["body"]["searchResult"]["totalRecords"]
        totalPages = response["body"]["searchResult"]["totalPages"]
        pageIndex = response["body"]["searchResult"]["pageIndex"]
        searchId = response["body"]["searchResult"]["searchId"]
        if isSuccess == True:
            if totalRecords > 0:
                if advance:
                    records = response["body"]["searchResult"]["searchRowList"]["searchRow"]
                else:
                    records = response["body"]["searchResult"]["recordList"]["record"]
                self.logger.info("TotalRecords {0}: {1}/{2}".format(totalRecords, len(records), pageIndex))
                limitPages = totalPages if limitPages == 0 else min([limitPages, totalPages])
                while(pageIndex < limitPages):
                    pageIndex += 1
                    records.extend(self.searchMoreWithId(searchId, pageIndex, advance=advance))
                    self.logger.info("TotalRecords {0}: {1}/{2}".format(totalRecords, len(records), pageIndex))
                return records
            else:
                return None
        else:
            statusDetail = response["body"]["searchResult"]["status"]["statusDetail"]
            raise Exception(statusDetail)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def searchMoreWithId(self, searchId, pageIndex, advance=False):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.searchMoreWithId(searchId=searchId, pageIndex=pageIndex, _soapheaders=soapheaders)
        isSuccess = response["body"]["searchResult"]["status"]["isSuccess"]
        totalRecords = response["body"]["searchResult"]["totalRecords"]
        if isSuccess == True:
            if totalRecords > 0:
                if advance:
                    return response["body"]["searchResult"]["searchRowList"]["searchRow"]
                else:
                    return response["body"]["searchResult"]["recordList"]["record"]
            else:
                return []
        else:
            statusDetail = response["body"]["searchResult"]["status"]["statusDetail"]
            raise Exception(statusDetail)

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def getSelectValue(self, getSelectValueFieldDescription=None):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.getSelectValue(
            fieldDescription=getSelectValueFieldDescription,
            pageIndex=0,
            _soapheaders=soapheaders
        )
        return response.body.getSelectValueResult.baseRefList.baseRef

    @retry(reraise=True, wait=wait_exponential(multiplier=1, max=60), stop=stop_after_attempt(5))
    def getDataCenterUrls(self):
        soapheaders = {
            "tokenPassport": self.tokenPassport,
            "applicationInfo": self.applicationInfo
        }
        response = self.service.getDataCenterUrls(
            account=self._setting['ACCOUNT'],
            _soapheaders=soapheaders
        )
        return response.body.getDataCenterUrlsResult.dataCenterUrls

    @property
    def service(self):
        """SOAP Service.
        """
        version = self._setting.get('VERSION', self.version).replace('_0', '')
        return self.client.create_service(
            "{urn:platform_" + version + ".webservices.netsuite.com}NetSuiteBinding",
            "https://{account_id}.suitetalk.api.netsuite.com/services/NetSuitePort_{version}".format(
                account_id=self._setting['ACCOUNT'].lower().replace('_', '-'),
                version=version
            )
        )

class NSConnector(object):

    def __init__(self, setting=None, logger=None):
        self._setting = setting
        self.logger = logger
        self.adaptor = Adaptor(self._setting, self.logger)
        self._tz = self._setting.get("TIMEZONE", "UTC")

    @property
    def adaptor(self):
        return self._adaptor

    @adaptor.setter
    def adaptor(self, adaptor):
        self._adaptor = adaptor

    @property
    def timezone(self):
        return timezone(self._tz)

    @timezone.setter
    def timezone(self, tz):
        self._tz = tz

    def getDataType(self, dataType):
        return self.adaptor.getDataType(dataType)

    def getRecord(self, internalId, recordType):
        RecordRef = self.getDataType("ns0:RecordRef")
        recordRef = RecordRef(internalId=internalId, type=recordType)
        return self.adaptor.get(baseRef=recordRef)

    def addRecord(self, record):
        return self.adaptor.add(record=record)

    def updateRecord(self, record):
        return self.adaptor.update(record=record)

    def search(self, searchRecord, searchPreferences=None, advance=False):
        return self.adaptor.search(searchRecord=searchRecord, searchPreferences=searchPreferences, advance=advance)

    def _getDataCenterUrls(self):
        return self.adaptor.getDataCenterUrls()

    def _getSelectValues(self, field, recordType):
        GetSelectValueFieldDescription = self.getDataType("ns0:GetSelectValueFieldDescription")
        getSelectValueFieldDescription = GetSelectValueFieldDescription(
            field=field,
            recordType=recordType
        )
        selectValues = self.adaptor.getSelectValue(getSelectValueFieldDescription=getSelectValueFieldDescription)
        return selectValues

    def _getSelectValue(self, value, field, recordType):
        selectValues = list(filter(lambda selectValue: (selectValue.name.find(value)==0), self._getSelectValues(field, recordType)))
        if len(selectValues) > 0:
            return selectValues[0]
        else:
            raise Exception("Cannot find the select value({0}) with the field({1}) and recordType({2}).".format(value, field, recordType))

    def _getCustomListValues(self, internalId):
        record = self.getRecord(internalId, "customList")
        return record["customValueList"]["customValue"]
    
    def _getSelectedCustomListValues(self, values, internalId):
        selectedValues = list(filter(lambda x: x.value in values, self._getCustomListValues(internalId)))
        if len(selectedValues) > 0:
            return selectedValues
        else:
            raise Exception("Cannot find the select values({0}) with the internal({1}).".format(",".join(values), internalId))

    def _getEmployee(self, email):
        EmployeeSearchBasic = self.getDataType("ns5:EmployeeSearchBasic")
        SearchStringField =  self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")

        if email is not None:
            searchRecord = EmployeeSearchBasic(
                email=SearchStringField(searchValue=email.strip(), operator="is"),
                isInactive=SearchBooleanField(searchValue=False)
            )
            records = self.search(searchRecord)
            if records is not None:
                employee = records[0]
                self.logger.info("## Search By email({0}),  email/internalId: {1}/{2}.".format(email.strip(), employee.email, employee.internalId))
                return employee
        return None

    def _getVendor(self, email, entityId=None):
        vendorSearchBasic = self.getDataType("ns5:VendorSearchBasic")
        SearchStringField =  self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")

        ## Search Vendor by entityId.
        if entityId is not None:
            searchRecord = vendorSearchBasic(
                entityId=SearchStringField(searchValue=entityId.strip(), operator="is"),
                isInactive=SearchBooleanField(searchValue=False)
            )
            records = self.search(searchRecord)
            if records is not None:
                vendor = records[0]
                self.logger.info("## Search By email({0}),  email/internalId: {1}/{2}.".format(email.strip(), vendor.email, vendor.internalId))
                return vendor

        ## Search Vendor by email
        searchRecord = vendorSearchBasic(
            email=SearchStringField(searchValue=email.strip(), operator="is"),
            isInactive=SearchBooleanField(searchValue=False)
        )
        records = self.search(searchRecord)
        if records is not None:
            vendor = records[0]
            self.logger.info("## Search By email({0}),  email/internalId: {1}/{2}.".format(email.strip(), vendor.email, vendor.internalId))
            return vendor
        return None

    def _getCustomer(self, email, entityId=None, externalId=None, bodyFieldsOnly=True):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        CustomerSearchBasic = self.getDataType("ns5:CustomerSearchBasic")
        SearchStringField =  self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=bodyFieldsOnly)

        ## Search Customer by entityId.
        if entityId is not None:
            searchRecord = CustomerSearchBasic(
                entityId=SearchStringField(searchValue=entityId, operator="is"),
                isInactive=SearchBooleanField(searchValue=False)
            )
            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None and len(records) == 1:
                customer = records[0]
                self.logger.info("## Search By entityId({0}), entityId/internalId: {1}/{2}.".format(entityId, customer.entityId, customer.internalId))
                return customer
        
        ## Search Customer by exernalId.
        if externalId is not None:
            recordRef = RecordRef(externalId=externalId, type='customer')
            searchRecord = CustomerSearchBasic(
                externalId=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
                isInactive=SearchBooleanField(searchValue=False)
            )
            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None and len(records) == 1:
                customer = records[0]
                self.logger.info("## Search By externalId({0}), entityId/internalId: {1}/{2}.".format(externalId, customer.entityId, customer.internalId))
                return customer 

        ## Search Customer by email
        searchRecord = CustomerSearchBasic(
            email=SearchStringField(searchValue=email.strip(), operator="is"),
            isInactive=SearchBooleanField(searchValue=False)
        )
        records = self.search(searchRecord, searchPreferences=searchPreferences)
        if records is not None:
            customer = records[0]
            self.logger.info("## Search By email({0}),  email/internalId: {1}/{2}.".format(email.strip(), customer.email, customer.internalId))
            return customer

        self.logger.info("## Cannot find the customer by entityId({0}) or email({1}).".format(entityId, email))
        return None

    def _createCustomer(self, **params):
        Customer = self.getDataType("ns13:Customer")
        CustomerAddressbookList =self.getDataType("ns13:CustomerAddressbookList")
        CustomerAddressbook = self.getDataType("ns13:CustomerAddressbook")
        Address = self.getDataType("ns5:Address")

        firstName = params.get("firstName")
        lastName = params.get("lastName")
        email = params.get("email")
        externalId = params.get("externalId")
        address = params.get("address")

        record = self.addRecord(
            Customer(
                isPerson=True,
                firstName=firstName,
                lastName=lastName,
                companyName=address.get("attention", None),
                email=email,
                externalId=externalId,
                addressbookList=CustomerAddressbookList(
                    addressbook=[
                        CustomerAddressbook(
                            defaultShipping=True,
                            defaultBilling=True,
                            isResidential=address.get("isResidential", True),
                            label=address.get("addr1"),
                            addressbookAddress=Address(
                                country=address.get("country", "_unitedStates"),
                                attention=address.get("attention", None),
                                addressee="{0} {1}".format(firstName, lastName),
                                addrPhone=address.get("addrPhone", ""),
                                addr1=address.get("addr1"),
                                addr2=address.get("addr2", None),
                                addr3=address.get("addr3", None),
                                city=address.get("city"),
                                state=address.get("state"),
                                zip=address.get("zip")
                            )
                        )
                    ]
                )
            )
        )
        self.logger.info("## The customer({0}) is created with email({1}).".format(record["internalId"], email))
        return self.getRecord(record["internalId"], "customer")

    def _updateCustomer(self, customer):
        Customer = self.getDataType("ns13:Customer")
        CustomerAddressbookList =self.getDataType("ns13:CustomerAddressbookList")
        CustomerAddressbook = self.getDataType("ns13:CustomerAddressbook")
        Address = self.getDataType("ns5:Address")

        firstName = customer.get("firstName")
        lastName = customer.get("lastName")
        email = customer.get("email")
        externalId = customer.get("externalId")
        companyName = customer.get("companyName")

        customerAddressbook = [
            CustomerAddressbook(
                defaultShipping=address.get("defaultShipping", False),
                defaultBilling=address.get("defaultBilling", False),
                isResidential=address.get("isResidential", True),
                label=address.get("addr1"),
                addressbookAddress=Address(
                    country=address.get("country", "_unitedStates"),
                    attention=address.get("attention", None),
                    addressee="{0} {1}".format(customer["firstName"], customer["lastName"]),
                    addrPhone=address.get("addrPhone", ""),
                    addr1=address.get("addr1"),
                    addr2=address.get("addr2", None),
                    addr3=address.get("addr3", None),
                    city=address.get("city"),
                    state=address.get("state"),
                    zip=address.get("zip")
                )
            ) for address in customer["addresses"]
        ]

        _customer = Customer(
            isPerson=True,
            firstName=firstName,
            lastName=lastName,
            companyName=companyName,
            email=email,
            externalId=externalId,
            addressbookList=CustomerAddressbookList(
                addressbook=customerAddressbook
            )
        )

        cust = self._getCustomer(customer["email"])
        if cust is not None:
            _customer.internalId = cust.internalId
            record = self.updateRecord(_customer)
        else:
            record = self.addRecord(_customer)

        self.logger.info("{0}/{1} is updated.".format(_customer["email"], record["internalId"]))
        return record["internalId"]

    def insertCustomers(self, customers):
        for customer in customers:
            try:
                customer["bo_customer_id"] = self._updateCustomer(customer)
                customer["tx_status"] = 'S'
            except Exception:
                log = traceback.format_exc()
                customer["bo_customer_id"] = "####"
                customer["tx_status"] = 'F'
                customer["tx_note"] = log
                self.logger.exception('Failed to create customer: {0} with error: {1}'.format(customer, log))
        return customers

    def _getItem(self, id, bodyFieldsOnly=True, isInactive=None):
        self.logger.info("lookup item({id}) by ItemId.".format(id=id))
        item = self._getItemByItemId(id, bodyFieldsOnly=bodyFieldsOnly, isInactive=isInactive)
        if item is None:
            self.logger.info("lookup item({id}) by MPN.".format(id=id))
            item = self._getItemByMPN(id, bodyFieldsOnly=bodyFieldsOnly, isInactive=isInactive)
        return item

    def _getItemByItemId(self, itemId, bodyFieldsOnly=True, isInactive=None):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
        SearchStringField = self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        searchRecord = ItemSearchBasic(
            itemId=SearchStringField(searchValue=itemId, operator="is"),
        )
        if isInactive is not None:
            searchRecord.isInactive = SearchBooleanField(searchValue=isInactive)

        searchPreferences = SearchPreferences(bodyFieldsOnly=bodyFieldsOnly)
        records = self.search(searchRecord, searchPreferences=searchPreferences)
        if records is not None:
            return records[0]
        else:
            self.logger.error("The item({0}) is not found.".format(itemId))
            return None

    def _getItemByInternalId(self, internalId, bodyFieldsOnly=True):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")
        recordRef = RecordRef(internalId=internalId)
        searchRecord = ItemSearchBasic(
            internalId=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf")
        )

        searchPreferences = SearchPreferences(bodyFieldsOnly=bodyFieldsOnly)
        records = self.search(searchRecord, searchPreferences=searchPreferences)
        if records is not None:
            return records[0]
        else:
            self.logger.error("The item({0}) is not found.".format(internalId))
            return None

    def _getItemByMPN(self, mpn, bodyFieldsOnly=True, isInactive=None):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
        SearchStringField = self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        searchRecord = ItemSearchBasic(
            mpn=SearchStringField(searchValue=mpn, operator="is"),
        )
        if isInactive is not None:
            searchRecord.isInactive = SearchBooleanField(searchValue=isInactive)

        searchPreferences = SearchPreferences(bodyFieldsOnly=bodyFieldsOnly)
        records = self.search(searchRecord, searchPreferences=searchPreferences)
        if records is not None:
            return records[0]
        else:
            self.logger.error("The item({0}) is not found.".format(mpn))
            return None

    def _getPriceLevel(self, priceLevel):
        PriceLevelSearchBasic = self.getDataType("ns5:PriceLevelSearchBasic")
        SearchStringField = self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        searchRecord = PriceLevelSearchBasic(
            name=SearchStringField(searchValue=priceLevel, operator="is"),
            isInactive=SearchBooleanField(searchValue=False)
        )

        records = self.search(searchRecord)
        if records is not None:
            self.logger.info("price_level({0}): {1}/{2}.".format(priceLevel, records[0].name, records[0].internalId))
            return records[0]
        else:
            self.logger.error("The price level({0}) is not found.".format(priceLevel))
            return None

    def insertSalesOrder(self, order):
        RecordRef = self.getDataType("ns0:RecordRef")
        SalesOrder = self.getDataType("ns19:SalesOrder")
        SalesOrderItemList = self.getDataType("ns19:SalesOrderItemList")
        SalesOrderItem = self.getDataType("ns19:SalesOrderItem")
        Address = self.getDataType("ns5:Address")
        CustomFieldList = self.getDataType("ns0:CustomFieldList")
        StringCustomFieldRef = self.getDataType("ns0:StringCustomFieldRef")
        SelectCustomFieldRef = self.getDataType("ns0:SelectCustomFieldRef")
        MultiSelectCustomFieldRef = self.getDataType("ns0:MultiSelectCustomFieldRef")
        ListOrRecordRef = self.getDataType("ns0:ListOrRecordRef")

        self.logger.info(order)

        _firstName = order.get("firstName")
        _lastName = order.get("lastName")
        _entityId = order.get("bo_customer_id", None)
        _externalId = order.get("fe_customer_id", None)
        _email = order.get("email")
        _items = order.get("items")
        _billingAddress = order.get("billingAddress")
        _shippingAddress = order.get("shippingAddress")
        _customFields = order.get("customFields", None)
        _terms = order.get("terms", None)
        _paymentMethod = order.get("paymentMethod", None)
        _shipMethod = order.get("shipMethod", None)
        _shippingCost = order.get("shippingCost", None)
        _otherRefNum = order.get("otherRefNum")
        _source = order.get("source")
        _memo = order.get("memo", "####")
        _priceLevel = self._getPriceLevel(order.get("priceLevel", None))
        _salesRep = self._getEmployee(order.get("salesRep", None))

        salesOrder = None
        message = None

        try:
            # Look up customer and create customer.
            customer = self._getCustomer(
                _email,
                entityId=_entityId,
                externalId=_externalId
            )
            if customer is None:
                customer = self._createCustomer(
                    **{
                        "firstName": _firstName,
                        "lastName": _lastName,
                        "email": _email,
                        "address": _billingAddress,
                        "externalId": _externalId
                    }
                )
            self.logger.info("Customer: {customer_email}/{customer_internal_id} by {email}/{external_id}".format(
                    customer_email=customer.email, 
                    customer_internal_id=customer.internalId,
                    email=_email,
                    external_id=_externalId
                )
            )

            # Items
            items = []
            for _item in _items:
                sku = _item.get("sku")
                qty = _item.get("qty")
                item = self._getItem(sku, bodyFieldsOnly=False, isInactive=False)
                if item is not None:
                    salesOrderItem = SalesOrderItem(
                        item=RecordRef(internalId=item.internalId),
                        quantity=qty,
                    )

                    # Calculate item price level or customized price.
                    if _priceLevel is not None and item.pricingMatrix is not None:
                        _prices = list(
                            filter(lambda p: (p['priceLevel']['internalId'] == _priceLevel.internalId), item.pricingMatrix.pricing)
                        )
                        if len(_prices) > 0:
                            _price = min(list(filter(lambda p: p["quantity"] is None or (p["quantity"]<=qty), _prices[0]['priceList']['price'])), key=lambda p: p["value"])                    
                            difference = float(_price['value']) - float(_item["price"]) if "price" in _item.keys() else 0   # If there is no price in the line item of the order, the price of the product will be used.
                        else:
                            difference = -1

                        if difference != 0 or _priceLevel.internalId == '5':
                            salesOrderItem.price = RecordRef(internalId=-1)
                            salesOrderItem.rate = _item["price"]
                        else:
                            salesOrderItem.price = RecordRef(internalId=_priceLevel.internalId)
                    else:
                        if "price" in _item.keys():
                            salesOrderItem.price = RecordRef(internalId=-1)
                            salesOrderItem.rate = _item["price"]
                    
                    # Calculate the subtotal for each line item.
                    if "price" in _item.keys():
                        salesOrderItem.amount = float(qty)*float(_item["price"])
                    items.append(salesOrderItem)
                    self.logger.info("The item({0}/{1}) is added.".format(sku, item.internalId))
                else:
                    log = "The item({0}) is removed since it cannot be found.".format(sku)
                    message = message + "\n" + log if message is not None else log
                    self.logger.info(log)

            # Billing Address
            billingAddress = Address(
                country=_billingAddress.get("country", "_unitedStates"),
                attention=_billingAddress.get("attention", None),
                addressee="{0} {1}".format(
                    _billingAddress.get("firstName"), _billingAddress.get("lastName")
                ),
                addrPhone=_billingAddress.get("addrPhone", None),
                addr1=_billingAddress.get("addr1"),
                addr2=_billingAddress.get("addr2", None),
                addr3=_billingAddress.get("addr3", None),
                city=_billingAddress.get("city"),
                state=_billingAddress.get("state"),
                zip=_billingAddress.get("zip"),
            )

            # Shipping Address
            shippingAddress = Address(
                country=_shippingAddress.get("country", "_unitedStates"),
                attention=_shippingAddress.get("attention", None),
                addressee="{0} {1}".format(
                    _shippingAddress.get("firstName"), _shippingAddress.get("lastName")
                ),
                addrPhone=_shippingAddress.get("addrPhone", None),
                addr1=_shippingAddress.get("addr1"),
                addr2=_shippingAddress.get("addr2", None),
                addr3=_shippingAddress.get("addr3", None),
                city=_shippingAddress.get("city"),
                state=_shippingAddress.get("state"),
                zip=_shippingAddress.get("zip"),
            )
            
            emailOrderTo = list(filter(lambda cf: cf.scriptId=='custentity_bsp_mcc_emailordersto' and (cf.value is not None or cf.value != ''), customer.customFieldList.customField))
            current = datetime.now(tz=self.timezone)
            _salesOrder = SalesOrder(
                entity=RecordRef(internalId=customer.internalId),
                email=emailOrderTo[0].value if len(emailOrderTo) == 1 else customer.email,
                tranDate=current + timedelta(hours=24),
                billingAddress=billingAddress,
                shippingAddress=shippingAddress,
                otherRefNum=_otherRefNum,
                source=_source,
                memo=_memo if _memo != "####" else "",
                itemList=SalesOrderItemList(
                    item=items,
                    replaceAll=False
                ),
            )

            # Customer Fields
            customFields = []
            if _customFields is not None:
                selectCustomFieldValues = self._setting["NETSUITEMAPPINGS"]["selectCustomFieldValues"] \
                    if "NETSUITEMAPPINGS" in self._setting.keys() and "selectCustomFieldValues" in self._setting['NETSUITEMAPPINGS'].keys() else None
                for scriptId, value in _customFields.items():
                    if selectCustomFieldValues is not None and scriptId in selectCustomFieldValues:
                        if type(value) is list:
                            if len(value) > 0:
                                customFields.append(
                                    MultiSelectCustomFieldRef(
                                        scriptId=scriptId,
                                        value=[ListOrRecordRef(internalId=self._getSelectValue(i, scriptId, "salesOrder").internalId) for i in value]
                                    )
                                )
                            else:
                                pass
                        else:
                            customFields.append(
                                SelectCustomFieldRef(
                                    scriptId=scriptId,
                                    value=ListOrRecordRef(
                                        internalId=self._getSelectValue(value, scriptId, "salesOrder").internalId
                                    )
                                )
                            )
                    else:
                        customFields.append(
                            StringCustomFieldRef(
                                scriptId=scriptId,
                                value=value
                            )
                        )

            if len(customFields) != 0:
                _salesOrder.customFieldList=CustomFieldList(
                    customField=customFields
                )

            if _terms is not None:
                _salesOrder.terms = RecordRef(
                    internalId=self._getSelectValue(_terms, "terms", "salesOrder").internalId
                )

            if _paymentMethod is not None:
                _salesOrder.paymentMethod = RecordRef(
                    internalId=self._getSelectValue(_paymentMethod, "paymentmethod", "salesOrder").internalId
                )

            if _shipMethod is not None:
                _salesOrder.shipMethod = RecordRef(
                    internalId=self._getSelectValue(_shipMethod, "shipmethod", "salesOrder").internalId
                )

            if _shippingCost is not None:
                _salesOrder.shippingCost = float(_shippingCost)

            # Check if message is None or not.
            if message is not None:
                _salesOrder.message = message

            # Check salesRep is None or not.
            if _salesRep is not None:
                _salesOrder.salesRep = RecordRef(internalId=_salesRep.internalId)

            record = self.addRecord(_salesOrder)
            salesOrder = self.getRecord(record["internalId"], "salesOrder")
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return salesOrder.tranId

    def insertOrders(self, orders):
        for order in orders:
            try:
                salesOrder = self.getSalesOrder(otherRefNum=order["otherRefNum"])
                order["bo_order_id"] = self.insertSalesOrder(order) if salesOrder is None else salesOrder.tranId
                order["tx_status"] = 'S'
            except Exception:
                log = traceback.format_exc()
                order["bo_order_id"] = "####"
                order["tx_status"] = 'F'
                order["tx_note"] = log
                self.logger.exception('Failed to create order: {0} with error: {1}'.format(order, log))
        return orders

    def _getLineItems(self, internalIds):
        ## The limitation for multiselect search is 1000.
        records = []
        for i in range(0, len(internalIds), 500):
            ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
            SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")
            searchRecord = ItemSearchBasic(
                internalId=SearchMultiSelectField(searchValue=internalIds[i:i+500], operator="anyOf")
            )
            records.extend(self.search(searchRecord))

        items = {}
        for item in records:
            internalId = item["internalId"]
            items[internalId] = item
        return items

    def _updateLineItems(self, record):
        RecordRef = self.getDataType("ns0:RecordRef")
        internalIds = []
        for i in range(0, len(record["itemList"]["item"])):
            internalId = record["itemList"]["item"][i]["item"]["internalId"]
            internalIds.append(RecordRef(internalId=internalId))

        items = self._getLineItems(internalIds)
        for i in range(0, len(record["itemList"]["item"])):
            internalId = record["itemList"]["item"][i]["item"]["internalId"]
            record["itemList"]["item"][i]["item"] = items[internalId]

    def getCustomers(self, **params):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        CustomerSearchBasic = self.getDataType("ns5:CustomerSearchBasic")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        SearchDateField = self.getDataType("ns0:SearchDateField")

        cutDt = params.get("cutDt")
        endDt = params.get("endDt")
        limit = params.get("limit", 100)
        hours = params.get("hours", 0)

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)

        begin = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        if hours == 0:
            end = datetime.strptime(endDt, "%Y-%m-%d %H:%M:%S")
        else:
            end = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(hours=hours)
        try:
            customers = []
            searchRecord = CustomerSearchBasic(
                isInactive=SearchBooleanField(searchValue=False),
                lastModifiedDate=SearchDateField(
                    searchValue=begin,
                    searchValue2=end,
                    operator="within"
                )
            )
            self.logger.info("Begin: {}".format(begin.strftime("%Y-%m-%d %H:%M:%S")))
            self.logger.info("End: {}".format(end.strftime("%Y-%m-%d %H:%M:%S")))

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                records = sorted(records, key=lambda x: x['lastModifiedDate'], reverse=False)
                customers = records[:limit]
                for customer in customers:
                    if customer.salesRep is not None:
                        salesRepInternalId = customer.salesRep.internalId
                        customer.salesRep = self.getRecord(salesRepInternalId, "employee")
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return customers

    def getInvoices(self, **params):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchDateField = self.getDataType("ns0:SearchDateField")

        cutDt = params.get("cutDt")
        endDt = params.get("endDt")
        limit = params.get("limit", 100)
        hours = params.get("hours", 0)

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)

        begin = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        if hours == 0:
            end = datetime.strptime(endDt, "%Y-%m-%d %H:%M:%S")
        else:
            end = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(hours=hours)
        try:
            invoices = []
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["invoice"], operator="anyOf"),
                dateCreated=SearchDateField(
                    searchValue=begin,
                    searchValue2=end,
                    operator="within"
                )
            )
            self.logger.info("Begin: {}".format(begin.strftime("%Y-%m-%d %H:%M:%S")))
            self.logger.info("End: {}".format(end.strftime("%Y-%m-%d %H:%M:%S")))

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                records = sorted(records, key=lambda x: x['createdDate'], reverse=False)
                invoices = records[:limit]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return invoices

    def getSalesOrder(self, tranId=None, otherRefNum=None):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchTextNumberField = self.getDataType("ns0:SearchTextNumberField")
        SearchStringField =  self.getDataType("ns0:SearchStringField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        try:
            salesOrder = None
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["salesOrder"], operator="anyOf")
            )

            if otherRefNum is not None:
                searchRecord.otherRefNum = SearchTextNumberField(searchValue=otherRefNum, operator="equalTo")
            else:
                searchRecord.tranId = SearchStringField(searchValue=tranId, operator="is")

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                salesOrder = records[0]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return salesOrder

    def getInvoice(self, tranId):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchStringField =  self.getDataType("ns0:SearchStringField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        try:
            invoice = None
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["invoice"], operator="anyOf"),
                tranId=SearchStringField(searchValue=tranId, operator="is")
            )

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                invoice = records[0]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return invoice

    def getTransactionByInternalId(self, internalId, txType):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")
        RecordRef = self.getDataType("ns0:RecordRef")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        try:
            recordRef = RecordRef(internalId=internalId)        
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=[txType], operator="anyOf"),
                internalId=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf")
            )

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                transaction = records[0]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return transaction

    def getPurchaseOrder(self, tranId):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchStringField =  self.getDataType("ns0:SearchStringField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        try:
            purchaseOrder = None
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["purchaseOrder"], operator="anyOf"),
                tranId=SearchStringField(searchValue=tranId, operator="is")
            )

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                purchaseOrder = records[0]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return purchaseOrder

    def getSalesOrders(self, **params):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchDateField = self.getDataType("ns0:SearchDateField")

        cutDt = params.get("cutDt")
        endDt = params.get("endDt")
        join = params.get("join")
        limit = params.get("limit", 100)
        hours = params.get("hours", 0)

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)

        begin = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        if hours == 0:
            end = datetime.strptime(endDt, "%Y-%m-%d %H:%M:%S")
        else:
            end = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(hours=hours)
        try:
            salesOrders = []
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["salesOrder"], operator="anyOf"),
                lastModifiedDate=SearchDateField(
                    searchValue=begin,
                    searchValue2=end,
                    operator="within"
                )
            )
            
            self.logger.info("Begin: {}".format(begin.strftime("%Y-%m-%d %H:%M:%S")))
            self.logger.info("End: {}".format(end.strftime("%Y-%m-%d %H:%M:%S")))

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                records = sorted(records, key=lambda x: x['lastModifiedDate'], reverse=True)
                while (len(records)):
                    record = records.pop()
                    try:
                        for entityType, value in join.items():
                            if entityType == 'itemfulfillment':
                                self._joinEntity(record, value, funct=self._getItemFulfillmentsBySalesOrder)
                    except Exception:
                        log = traceback.format_exc()
                        self.logger.exception(log)
                    salesOrders.append(record)
                    if len(salesOrders) >= limit and salesOrders[len(salesOrders)-1]['lastModifiedDate'] != records[len(records)-1]['lastModifiedDate']:
                        break
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return salesOrders

    def _joinEntity(self, record, value, funct=None):
        entities = funct(record)

        if entities is not None:
            for field in value['base']:
                cols = field.split('|')
                record[cols[0]] = entities[0][cols[1]]
            
            for item in record.itemList.item:
                for entity in entities:
                    x = list(filter(lambda t: (t.item.internalId==item.item.internalId), entity.itemList.item))
                    if len(x) > 0:
                        for field in value['lines']:
                            cols = field.split('|')
                            item[cols[0]] = x[0][cols[1]]

    def getPurchaseOrders(self, **params):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")
        SearchDateField = self.getDataType("ns0:SearchDateField")

        vendorId = params.get("vendorId")
        cutDt = params.get("cutDt")
        endDt = params.get("endDt")
        itemDetail = params.get("itemDetail", False)
        join = params.get("join")
        limit = params.get("limit", 100)
        hours = params.get("hours", 0)

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)

        begin = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        if hours == 0:
            end = datetime.strptime(endDt, "%Y-%m-%d %H:%M:%S")
        else:
            end = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(hours=hours)
        try:
            purchaseOrders = []
            searchRecord = TransactionSearchBasic(
                type=SearchEnumMultiSelectField(searchValue=["purchaseOrder"], operator="anyOf"),
                lastModifiedDate=SearchDateField(
                    searchValue=begin,
                    searchValue2=end,
                    operator="within"
                )
            )

            if vendorId is not None:
                recordRef = RecordRef(internalId=vendorId)
                searchRecord.entity = SearchMultiSelectField(searchValue=[recordRef], operator="anyOf")
            
            self.logger.info("Begin: {}".format(begin.strftime("%Y-%m-%d %H:%M:%S")))
            self.logger.info("End: {}".format(end.strftime("%Y-%m-%d %H:%M:%S")))

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                records = sorted(records, key=lambda x: x['lastModifiedDate'], reverse=True)
                while (len(records)):
                    record = records.pop()
                    try:
                        if itemDetail:
                            self._updateLineItems(record)

                        for entityType, value in join.items():
                            if entityType == 'vendorbill':
                                self._joinEntity(record, value, funct=self._getVendorBillsByPurchaseOrder)
                    except Exception:
                        log = traceback.format_exc()
                        self.logger.exception(log)
                    purchaseOrders.append(record)
                    if len(purchaseOrders) >= limit and purchaseOrders[len(purchaseOrders)-1]['lastModifiedDate'] != records[len(records)-1]['lastModifiedDate']:
                        break
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return purchaseOrders

    def _getPurchaseOrdersBySalesOrder(self, salesOrder):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        recordRef = RecordRef(internalId=salesOrder.internalId, type="salesOrder")
        searchRecord = TransactionSearchBasic(
            type=SearchEnumMultiSelectField(searchValue=["purchaseOrder"], operator="anyOf"),
            createdFrom=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
        )

        records = self.search(searchRecord, searchPreferences=searchPreferences)
        if records is not None:
            return records
        else:
            raise Exception("Cannot find the reference records{}.".format(salesOrder.tranId))

    def _getItemFulfillmentsBySalesOrder(self, salesOrder):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        recordRef = RecordRef(internalId=salesOrder.internalId, type="salesOrder")
        searchRecord = TransactionSearchBasic(
            type=SearchEnumMultiSelectField(searchValue=["itemFulfillment"], operator="anyOf"),
            createdFrom=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
        )

        return self.search(searchRecord, searchPreferences=searchPreferences)

    def _getVendorBillsByPurchaseOrder(self, purchaseOrder):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        RecordRef = self.getDataType("ns0:RecordRef")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")

        searchPreferences = SearchPreferences(bodyFieldsOnly=False)
        recordRef = RecordRef(internalId=purchaseOrder.internalId, type="purchaseOrder")
        searchRecord = TransactionSearchBasic(
            type=SearchEnumMultiSelectField(searchValue=["vendorBill"], operator="anyOf"),
            createdFrom=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
        )

        return self.search(searchRecord, searchPreferences=searchPreferences)

    def _getItemfromPurchaseOrder(self, purchaseOrders, poInternalId, itemInternalId):
        for purchaseOrder in purchaseOrders:
            if purchaseOrder.internalId == poInternalId:
                # Match the item between purchase order and sales order.
                x = list(filter(lambda t: (t.item.internalId==itemInternalId), purchaseOrder.itemList.item))
                if len(x) == 1:
                    item = x[0]
                    self.logger.info("Purchase Order: {}/{}, sku: {}, quantity: {}, quantityReceived: {}".format(
                        purchaseOrder.tranId,
                        purchaseOrder.internalId,
                        item["item"]["name"],
                        item["quantity"],
                        item["quantityReceived"])
                    )
                    return item
        return None

    def insertItemReceipt(self, itemReceipt):
        RecordRef = self.getDataType("ns0:RecordRef")
        ItemReceipt = self.getDataType("ns21:ItemReceipt")
        ItemReceiptItemList = self.getDataType("ns21:ItemReceiptItemList")
        ItemReceiptItem = self.getDataType("ns21:ItemReceiptItem")
        ItemFulfillment = self.getDataType("ns19:ItemFulfillment")
        ItemFulfillmentItemList = self.getDataType("ns19:ItemFulfillmentItemList")
        ItemFulfillmentItem = self.getDataType("ns19:ItemFulfillmentItem")
        TransactionSearchBasic = self.getDataType("ns5:TransactionSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchMultiSelectField = self.getDataType("ns0:SearchMultiSelectField")

        self.logger.info(itemReceipt)
        internalIds = None

        try:
            if type(itemReceipt["ref"]) is list:
                salesOrder = self.getRecord(itemReceipt["internalId"], "salesOrder")
            else:
                purchaseOrder = self.getRecord(itemReceipt["internalId"], "purchaseOrder")
                self.logger.info("Purchase Order: {}/{}".format(purchaseOrder.tranId, purchaseOrder.internalId))
                salesOrder = self.getRecord(purchaseOrder.createdFrom.internalId, "salesOrder") \
                    if purchaseOrder.createdFrom is not None else None

            # Send ItemFulfillment if the PurchaseOrder is converted from a SalesOrder.
            if salesOrder is not None:
                self.logger.info("Sales Order: {}/{}".format(salesOrder.tranId, salesOrder.internalId))
                purchaseOrders = self._getPurchaseOrdersBySalesOrder(salesOrder)
                # Insert a new itemFulfillment.
                items = []
                sendReq = False
                for item in salesOrder.itemList.item:
                    i = list(filter(lambda t: (t["sku"]==item.item.name), itemReceipt["items"]))  # Match the item between sales order and source data
                    qty = float(i[0]["qty"]) if len(i) >= 1 else 0
                    self.logger.info("sku: {}/{}".format(item.item.name, item.item.internalId))
                    # self.logger.info(item)
                    if item.createdPo is not None:
                        _item = self._getItemfromPurchaseOrder(purchaseOrders, item.createdPo.internalId, item.item.internalId)
                        if (_item is not None) and (_item["quantity"] - _item["quantityReceived"] != 0):
                            sendReq = True if qty > 0 else sendReq
                            items.append(
                                ItemFulfillmentItem(
                                    item=RecordRef(internalId=item.item.internalId),
                                    quantity=qty,
                                    orderLine=item.line
                                )
                            )

                if sendReq:
                    self.logger.info("Insert a new item fulfillment")
                    # self.logger.info(items)
                    record = self.addRecord(ItemFulfillment(
                            createdFrom=RecordRef(internalId=salesOrder.internalId, type="salesOrder"),
                            tranDate=datetime.now(tz=self.timezone),
                            itemList=ItemFulfillmentItemList(
                                item=items,
                                replaceAll=False
                            )
                        )
                    )
                    internalIds = [record["internalId"]]
                else:
                    recordRef = RecordRef(internalId=salesOrder.internalId, type="salesOrder")
                    searchRecord = TransactionSearchBasic(
                        type=SearchEnumMultiSelectField(searchValue=["itemFulfillment"], operator="anyOf"),
                        createdFrom=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
                    )

                    records = self.search(searchRecord)
                    if records is not None:
                        internalIds = [record["internalId"] for record in records]
                    else:
                        raise Exception("Cannot find the reference record{}.".format(itemReceipt["bo_po_num"]))
            # Send ItemReceipt if the PurchaseOrder is generated directly.
            else:
                items = []
                sendReq = False
                for item in purchaseOrder.itemList.item:
                    if item.quantity - item.quantityReceived != 0:
                        i = list(filter(lambda t: (t["sku"]==item.item.name), itemReceipt["items"]))  # Match the item between purchase order and source data.
                        qty = float(i[0]["qty"]) if len(i) >= 1 else 0
                        sendReq = True if qty > 0 else sendReq
                        items.append(ItemReceiptItem(
                                item=RecordRef(internalId=item.item.internalId),
                                quantity=qty,
                                orderLine=item.line
                            )
                        )

                if sendReq:
                    self.logger.info("Insert a new item receipt")
                    self.logger.info(items)
                    record = self.addRecord(ItemReceipt(
                            createdFrom=RecordRef(internalId=itemReceipt["internalId"], type="purchaseOrder"),
                            tranDate=datetime.now(tz=self.timezone),
                            itemList=ItemReceiptItemList(
                                item=items,
                                replaceAll=False
                            )
                        )
                    )
                    internalIds = [record["internalId"]]
                else:
                    recordRef = RecordRef(internalId=itemReceipt["internalId"], type="purchaseOrder")
                    searchRecord = TransactionSearchBasic(
                        type=SearchEnumMultiSelectField(searchValue=["itemReceipt"], operator="anyOf"),
                        createdFrom=SearchMultiSelectField(searchValue=[recordRef], operator="anyOf"),
                    )

                    records = self.search(searchRecord)
                    if records is not None:
                        internalIds = [record["internalId"] for record in records]
                    else:
                        raise Exception("Cannot find the reference record{}.".format(itemReceipt["bo_po_num"]))
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return internalIds

    def insertItemReceipts(self, itemReceipts):
        for itemReceipt in itemReceipts:
            if "bo_itemreceipt_id" not in itemReceipt.keys():
                itemReceipt["bo_itemreceipt_id"] = []
            try:
                internalIds = self.insertItemReceipt(itemReceipt)
                itemReceipt["bo_itemreceipt_id"].extend(internalIds)
                itemReceipt["tx_status"] = 'S'
            except Exception:
                log = traceback.format_exc()
                itemReceipt["tx_status"] = 'F'
                itemReceipt["tx_note"] = log
                self.logger.exception('Failed to create item receipt: {0} with error: {1}'.format(itemReceipt, e))
        return itemReceipts

    def _getLastQuantityAvailableChange(self, records, searchDateField):
        ItemSearchAdvanced = self.getDataType("ns17:ItemSearchAdvanced")
        ItemSearchRow = self.getDataType("ns17:ItemSearchRow")
        ItemSearchRowBasic = self.getDataType("ns5:ItemSearchRowBasic")
        ItemSearch = self.getDataType("ns17:ItemSearch")
        ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
        SearchColumnStringField = self.getDataType("ns0:SearchColumnStringField")
        SearchColumnDateField = self.getDataType("ns0:SearchColumnDateField")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")

        searchRecord = ItemSearchAdvanced(
            columns=ItemSearchRow(
                basic=ItemSearchRowBasic(
                    itemId=SearchColumnStringField(),
                    lastQuantityAvailableChange=SearchColumnDateField(),
                )
            ),
            criteria=ItemSearch(
                basic=ItemSearchBasic(
                    type=SearchEnumMultiSelectField(searchValue="inventoryItem", operator="anyOf"),
                    lastQuantityAvailableChange=searchDateField
                )
            )
        )
        rows = self.search(searchRecord, advance=True)
        for record in records:
            _rows = list(filter(lambda row: (row.basic.itemId[0].searchValue==record.itemId), rows))
            if len(_rows) > 0:
                record.lastModifiedDate = _rows[0].basic.lastQuantityAvailableChange[0].searchValue

    def getItems(self, **params):
        SearchPreferences = self.getDataType("ns4:SearchPreferences")
        ItemSearchBasic = self.getDataType("ns5:ItemSearchBasic")
        SearchEnumMultiSelectField = self.getDataType("ns0:SearchEnumMultiSelectField")
        SearchDateField = self.getDataType("ns0:SearchDateField")
        SearchStringField =  self.getDataType("ns0:SearchStringField")
        SearchBooleanField = self.getDataType("ns0:SearchBooleanField")
        SearchCustomFieldList = self.getDataType("ns0:SearchCustomFieldList")
        SearchStringCustomField = self.getDataType("ns0:SearchStringCustomField")
        SearchMultiSelectCustomField = self.getDataType("ns0:SearchMultiSelectCustomField")
        ListOrRecordRef = self.getDataType("ns0:ListOrRecordRef")

        cutDt = params.get("cutDt")
        endDt = params.get("endDt")
        dataType = params.get("dataType", "product")
        bodyFieldsOnly = params.get("bodyFieldsOnly", True)
        vendorName = params.get("vendorName", None)
        itemType = params.get("type", ["inventoryItem", "nonInventoryItem", "nonInventoryResaleItem"])
        limit = params.get("limit", 100)
        hours = params.get("hours", 0)
        activeOnly = params.get("activeOnly", False)
        customFields = params.get("customFields", None)

        searchPreferences = SearchPreferences(bodyFieldsOnly=bodyFieldsOnly)

        begin = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(seconds=1)
        if hours == 0:
            end = datetime.strptime(endDt, "%Y-%m-%d %H:%M:%S")
        else:
            end = datetime.strptime(cutDt, "%Y-%m-%d %H:%M:%S") + timedelta(hours=hours)
        try:
            items = []
            searchDateField = SearchDateField(
                searchValue=begin,
                searchValue2=end,
                operator="within"
            )
            if dataType == "inventory":
                searchRecord = ItemSearchBasic(
                    type=SearchEnumMultiSelectField(searchValue=["inventoryItem"], operator="anyOf"),
                    lastQuantityAvailableChange=searchDateField
                )
            else:
                searchRecord = ItemSearchBasic(
                    type=SearchEnumMultiSelectField(searchValue=itemType, operator="anyOf"),
                    lastModifiedDate=searchDateField
                )
            if vendorName is not None:
                searchRecord.vendorName = SearchStringField(searchValue=vendorName, operator="is")
            if activeOnly:
                searchRecord.isInactive=SearchBooleanField(searchValue=False)

            # Search Custom fields
            if customFields is not None:
                selectCustomFieldValues = self._setting["NETSUITEMAPPINGS"]["selectCustomFieldValues"] \
                    if "NETSUITEMAPPINGS" in self._setting.keys() and "selectCustomFieldValues" in self._setting['NETSUITEMAPPINGS'].keys() else None
                _customField = []
                for scriptId, value in customFields.items():
                    if selectCustomFieldValues is not None and scriptId in selectCustomFieldValues:
                        _customField.append(
                            SearchMultiSelectCustomField(
                                scriptId=scriptId,
                                searchValue=[ListOrRecordRef(internalId=self._getSelectValue(i, scriptId, itemType[0]).internalId) for i in value],
                                operator="anyOf"
                            )
                        )
                    else:
                        _customField.append(
                            SearchStringCustomField(scriptId=scriptId, searchValue=value, operator="is")
                        )
                searchRecord.customFieldList=SearchCustomFieldList(customField=_customField)

            self.logger.info("Begin: {}".format(begin.strftime("%Y-%m-%d %H:%M:%S")))
            self.logger.info("End: {}".format(end.strftime("%Y-%m-%d %H:%M:%S")))

            records = self.search(searchRecord, searchPreferences=searchPreferences)
            if records is not None:
                if dataType == "inventory":
                    self._getLastQuantityAvailableChange(records, searchDateField)
                records = sorted(records, key=lambda x: x['lastModifiedDate'], reverse=True)
                while (len(records)):
                    record = records.pop()
                    items.append(record)
                    if len(items) >= limit and items[len(items)-1]['lastModifiedDate'] != records[len(records)-1]['lastModifiedDate']:
                        break
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
        return items

    def insertItem(self, data, itemType="inventoryItem", msrpPriceLevel=None):
        RecordRef = self.getDataType("ns0:RecordRef")
        Item = self.getDataType("ns17:InventoryItem")
        if itemType == "nonInventoryResaleItem":
            Item = self.getDataType("ns17:NonInventoryResaleItem")
        PricingMatrix = self.getDataType("ns17:PricingMatrix")
        Pricing = self.getDataType("ns17:Pricing")
        PriceList = self.getDataType("ns17:PriceList")
        Price = self.getDataType("ns17:Price")
        CustomFieldList = self.getDataType("ns0:CustomFieldList")
        StringCustomFieldRef = self.getDataType("ns0:StringCustomFieldRef")
        SelectCustomFieldRef = self.getDataType("ns0:SelectCustomFieldRef")
        MultiSelectCustomFieldRef = self.getDataType("ns0:MultiSelectCustomFieldRef")
        ListOrRecordRef = self.getDataType("ns0:ListOrRecordRef")
        ItemVendor = self.getDataType("ns17:ItemVendor")
        ItemVendorList = self.getDataType("ns17:ItemVendorList")

        _customFields = data.get("customFields", None)
        try:
            ## Lookup the product is created.
            _item = self._getItemByItemId(data["sku"])
            if _item is not None:
                _item.upcCode = '{:12}'.format(int(eval(data.get('upc'))))
                _item.mpn = data.get("mpn", "")
                _item.weight = data.get("weight", 0.1)
                _item.weightUnit = data.get("weight_unit", "lb")
                _item.cost = data.get("cost", "0")
                if "description" not in self._setting.get("PRESERVEDFIELDS", []):
                    _item.salesDescription = data.get("description", "")[:4000]
                _msrp = data.get("msrp", None) \
                    if "msrp" not in self._setting.get("PRESERVEDFIELDS") else None
            else:
                _item = Item(
                    itemId=data.get("sku"),
                    externalId=data.get("sku"),
                    upcCode='{:12}'.format(int(eval(data.get('upc')))),
                    mpn=data.get("mpn", ""),
                    weight=data.get("weight", 0.1),
                    weightUnit=data.get("weight_unit", "lb"),
                    salesDescription=data.get("description", "")[:4000],
                    cost=data.get("cost", "0")
                )
                _msrp = data.get("msrp", None)
                _vendorInternalId = data.get("vendor_internal_id", None)
                if _vendorInternalId is not None:
                    _itemVendorList = ItemVendorList(
                        itemVendor=[
                            ItemVendor(
                                vendor=RecordRef(internalId=_vendorInternalId),
                                preferredVendor=True
                            )
                        ]
                    )
                    _item.itemVendorList = _itemVendorList

            # Customer Fieldsmn
            customFields = []
            if _customFields is not None:
                selectCustomFieldValues = self._setting["NETSUITEMAPPINGS"]["selectCustomFieldValues"] \
                    if "NETSUITEMAPPINGS" in self._setting.keys() and "selectCustomFieldValues" in self._setting['NETSUITEMAPPINGS'].keys() else None
                for scriptId, value in _customFields.items():
                    if selectCustomFieldValues is not None and scriptId in selectCustomFieldValues:
                        if type(value) is list:
                            if len(value) > 0:
                                customFields.append(
                                    MultiSelectCustomFieldRef(
                                        scriptId=scriptId,
                                        value=[ListOrRecordRef(internalId=self._getSelectValue(i, scriptId, itemType).internalId) for i in value]
                                    )
                                )
                            else:
                                pass
                        else:
                            customFields.append(
                                SelectCustomFieldRef(
                                    scriptId=scriptId,
                                    value=ListOrRecordRef(
                                        internalId=self._getSelectValue(value, scriptId, itemType).internalId
                                    )
                                )
                            )
                    else:
                        customFields.append(
                            StringCustomFieldRef(
                                scriptId=scriptId,
                                value=value[:4000]
                            )
                        )

            if len(customFields) != 0:
                if _item.customFieldList is None:
                    _item.customFieldList=CustomFieldList(
                        customField=customFields
                    )
                else:
                    for i in range(0, len(_item.customFieldList.customField)-1):
                        _updateCustomFields = list(filter(lambda t: (t.scriptId==_item.customFieldList.customField[i].scriptId), customFields))
                        if len(_updateCustomFields) >= 1 and \
                            _updateCustomFields[0].scriptId not in self._setting.get("PRESERVEDFIELDS", []):
                            _item.customFieldList.customField[i].value = _updateCustomFields[0].value

                    scriptIds = [customField.scriptId for customField in _item.customFieldList.customField]
                    _addCustomFields = [customField for customField in customFields if customField.scriptId not in scriptIds]
                    _item.customFieldList.customField.extend(_addCustomFields)

            if _msrp is not None and msrpPriceLevel is not None:
                _pricing = Pricing(
                    currency=None,
                    priceLevel=RecordRef(internalId=self._getPriceLevel(msrpPriceLevel).internalId),
                    discount=None,
                    priceList=PriceList(
                        price=[
                            Price(
                                value=_msrp,
                                quantity=None
                            )
                        ]
                    )
                )
                if _item.pricingMatrix is None:
                    pricingMatrix=PricingMatrix(
                        pricing=[_pricing]
                    )
                    _item.pricingMatrix = pricingMatrix
                else:
                    _item.pricingMatrix.pricing = [
                        Pricing(
                            currency=pricing.currency,
                            priceLevel=pricing.priceLevel,
                            discount=pricing.discount,
                            priceList=_pricing.priceList if pricing.priceLevel.internalId == _pricing.priceLevel.internalId else pricing.priceList
                        ) for pricing in _item.pricingMatrix.pricing
                    ]

            if _item.internalId is None:
                record = self.addRecord(_item)
                self.logger.info("{0}/{1} is inserted.".format(data["sku"], record["internalId"]))
            else:
                _item.lastModifiedDate = None
                record = self.updateRecord(_item)
                self.logger.info("{0}/{1} is updated.".format(data["sku"], record["internalId"]))
            return record["internalId"]
        except Exception:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise
