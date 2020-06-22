from .s3agency import S3Agency

class S3Agent(S3Agency):
    def __init__(self, setting={}, logger=None, feApp='S3', boApp='S3', dataWald=None, feConn=None, boConn=None):
        S3Agency.__init__(self, setting=setting, logger=logger, feApp=feApp, boApp=boApp, dataWald=dataWald, feConn=feConn, boConn=boConn)

    def boProductsExtFt(self, products, rawProducts):
        pass

    def boProductsExtDataExtFt(self, dataType, ProductsExtData, rawProductsExtData):
        pass

    def feItemReceiptsExtFt(self, itemReceipts, rawItemReceipts):
        pass
