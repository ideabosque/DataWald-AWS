from bcagency import BCAgency


class BCAgent(BCAgency):
    def __init__(self, setting=None, logger=None, feApp='bc', boApp='bc', dataWald=None, feConn=None, boConn=None):
        BCAgency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def feOrdersExtFt(self, orders, rawOrders):
        pass
