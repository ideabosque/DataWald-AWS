from .sqsagency import SQSAgency

class SQSAgent(SQSAgency):
    def __init__(self, setting={}, logger=None, feApp='sqs', boApp='sqs', dataWald=None, feConn=None, boConn=None):
        SQSAgency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def feOrdersExtFt(self, orders, rawOrders):
        pass
