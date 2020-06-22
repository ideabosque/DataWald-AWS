from .nsagency import NSAgency

class NSAgent(NSAgency):
    def __init__(self, setting={}, logger=None, feApp='NetSuite', boApp='NetSuite', dataWald=None, feConn=None, boConn=None):
        NSAgency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def boPurchaseOrdersExtFt(self, purchaseOrders, rawPurchaseOrders):
        pass

    def boItemReceiptExtFt(self, ir, itemReceipt):
        pass
