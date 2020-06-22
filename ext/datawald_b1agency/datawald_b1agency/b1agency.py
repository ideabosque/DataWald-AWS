from datawald_backoffice import BackOffice
from datetime import datetime, timedelta

class B1Agency(BackOffice):
    def __init__(self, setting=None, logger=None, boApp=None, dataWald=None, boConn=None):
        self.b1 = boConn
        self.setting = setting
        self.logger = logger

        BackOffice.__init__(self, logger=logger, boApp=boApp, dataWald=dataWald)

    def getPaymentMethod(self, method):
        paymentMethodsMetrics = self.setting['PAYMENTMETHODS_METRICS']
        code = None
        if method in paymentMethodsMetrics:
            code = paymentMethodsMetrics[method]
        return code

    def getTaxCode(self, lineTotal, taxAmt, postcode, country):
        taxMetrics = self.setting['TAX_METRICS']
        postcode = postcode.split('-')[0]
        taxCode = taxMetrics['Exempt']
        for k, v in taxMetrics['code_rate'].items():
            if (abs(round(lineTotal * float(v['rate']) / 100 - taxAmt)) == 0) and \
                (int(postcode) in range(int(v['postcode'][0]), int(v['postcode'][-1])) or postcode in v['postcode']) and \
                (country == v['country']):
                taxCode = k
                break
        return taxCode

    def getTransportName(self, shipmentMethod):
        trnspName = None
        shipmentMethods = self.setting['SHIPMENTMETHODS']
        if shipmentMethod in shipmentMethods.keys():
            trnspName = shipmentMethods[shipmentMethod]
        else:
            for k, v in shipmentMethods.items():
                if shipmentMethod.find(k) != -1:
                    trnspName = v
                    break
        return trnspName

    def boOrderFt(self, order):
        o = {}
        o['id'] = order['id']
        o['doc_due_date'] = (datetime.utcnow() + timedelta(days=self.setting['DOCDUTEDATE'])).strftime("%Y-%m-%d %H:%M:%S")

        # If 'B1BPCARDCODE_LOOKUP' is True, lookup the card_code by SAP B1 RESTful api first and set to default if not found.
        # Elif 'B1BPCARDCODE_LOOKUP' is False, just use the default value.
        if self.setting['B1BPCARDCODE_LOOKUP']:
            o['card_code'] = self.setting['B1BPCARDCODE']
        else:
            o['card_code'] = self.setting['B1BPCARDCODE']

        # Setup Freight cost for an order.
        if self.setting['ITEMASFREIGHTCOST']:
            freight_item = {
                'sku': self.setting['EXPENSESFREIGHTNAME'],
                'qty': '1',
                'price': str(abs(order['shipping_amt'])),
                'tax_amt': order['shipping_tax_amt']
            }
            order['items'].append(freight_item)
        else:
            o['expenses_freightname'] = self.setting['EXPENSESFREIGHTNAME']
            o['expenses_linetotal'] = order['shipping_amt']
            o['expenses_taxcode'] = self.getTaxCode(
                                        float(order['shipping_amt']),
                                        float(order['shipping_tax_amt']),
                                        order['addresses'][self.setting['TAXPOSTCODE']]['postcode'],
                                        order['addresses'][self.setting['TAXCOUNTRY']]['country']
                                    )
        if order['discount_amt'] != 0:
            o['discount_percent'] = (abs(float(order['discount_amt'])) / float(order['sub_total'])) * 100

        transportName = self.getTransportName(order['shipment_method'])
        if transportName is not None:
            o['transport_name'] = transportName

        paymentMethodCode = self.getPaymentMethod(order['payment_method'])
        if paymentMethodCode is not None:
            o['payment_method'] = paymentMethodCode

        o['fe_order_id'] = order['fe_order_id']
        if 'FEORDERIDUDF' in self.setting.keys() and self.setting['FEORDERIDUDF'] != '':
            o['fe_order_id_udf'] = self.setting['FEORDERIDUDF']

        # Set bill to address properties
        # order.AddressExtension.BillToBlock = "BillToBlockU"
        # order.AddressExtension.BillToBuilding = "BillToBuildingU"
        o['billto_firstname'] = order['addresses']['billto']['firstname']
        o['billto_lastname'] = order['addresses']['billto']['lastname']
        o['billto_email'] = order['addresses']['billto']['email']
        o['billto_companyname'] = order['addresses']['billto']['companyname'] \
            if 'companyname' in (order['addresses']['billto']).keys() else ""
        o['billto_city'] = order['addresses']['billto']['city']
        o['billto_country'] = order['addresses']['billto']['country']
        if o['billto_country'] in self.setting['B1SUPPORTCOUNTRIES']:
            o['billto_county'] = ''
            o['billto_state'] = order['addresses']['billto']['region']
        else:
            o['billto_county'] = order['addresses']['billto']['region']
            o['billto_state'] = ''
        o['billto_address'] = order['addresses']['billto']['address']
        # order.AddressExtension.BillToStreetNo = "ShipToStreetNoU"
        o['billto_zipcode'] = order['addresses']['billto']['postcode']
        o['billto_telephone'] = order['addresses']['billto']['telephone']
        # order.AddressExtension.BillToAddressType = "BillToAddressTypeU"

        # Set ship to address properties
        # order.AddressExtension.ShipToBlock = "ShipToBlockU"
        # order.AddressExtension.ShipToBuilding = "ShipToBuildingU"
        o['shipto_firstname'] = order['addresses']['shipto']['firstname']
        o['shipto_lastname'] = order['addresses']['shipto']['lastname']
        o['shipto_companyname'] = order['addresses']['shipto']['companyname'] \
            if 'companyname' in (order['addresses']['shipto']).keys() else ""
        o['shipto_city'] = order['addresses']['shipto']['city']
        o['shipto_country'] = order['addresses']['shipto']['country']
        if o['shipto_country'] in self.setting['B1SUPPORTCOUNTRIES']:
            o['shipto_county'] = ''
            o['shipto_state'] = order['addresses']['shipto']['region']
        else:
            o['shipto_county'] = order['addresses']['shipto']['region']
            o['shipto_state'] = ''
        o['shipto_address'] = order['addresses']['shipto']['address']
        # order.AddressExtension.ShipToStreetNo = "ShipToStreetNoU"
        o['shipto_zipcode'] = order['addresses']['shipto']['postcode']
        o['shipto_telephone'] = order['addresses']['shipto']['telephone']
        o['items'] = []
        return o

    def boOrderLineFt(self, i, o, item, order):
        lineTotal = float(item['qty']) * float(item['price'])
        taxAmt = 0 if (item['tax_amt'] is None or item['tax_amt'] == 'None') else float(item['tax_amt'])
        taxCode = self.getTaxCode(
                                    lineTotal,
                                    taxAmt,
                                    order['addresses'][self.setting['TAXPOSTCODE']]['postcode'],
                                    order['addresses'][self.setting['TAXCOUNTRY']]['country']
                                )
        lineItem = {
            'itemcode': item['sku'],
            'quantity': item['qty'],
            'price': item['price'],
            'taxcode': taxCode,
            'linetotal': lineTotal
        }
        o['items'].append(lineItem)

    def boOrderIdFt(self, order):
        feOrderId = order['fe_order_id']
        if 'FEORDERIDUDF' in self.setting.keys() and self.setting['FEORDERIDUDF'] != '':
            frontendIdUdf = self.setting['FEORDERIDUDF']
        else:
            frontendIdUdf = None
        boOrderId = self.b1.getBoOrderId(feOrderId,frontendIdUdf)
        return boOrderId

    def insertOrdersFt(self, newOrders):
        boOrders = self.b1.insertOrders(newOrders)
        for boOrder in boOrders:
            if boOrder['tx_status'] == 'F':
                log = "Fail to insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.error(log)
                self.logger.error(boOrder['tx_note'])
            else:
                log = "Successfully insert an order: %s/%s" % (boOrder['fe_order_id'], boOrder['bo_order_id'])
                self.logger.info(log)
        return boOrders
