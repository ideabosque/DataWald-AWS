from .dynamodbagency import DynamoDBAgency

class DynamoDBAgent(DynamoDBAgency):
    def __init__(self, setting={}, logger=None, feApp='dynamodb', boApp='dynamodb', dataWald=None, feConn=None, boConn=None):
        DynamoDBAgency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def boOrderExtFt(self, o, order):
        pass

    def boOrderLineExtFt(self, i, o, item, order):
        pass
