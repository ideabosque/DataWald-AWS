from b1agency import B1Agency


class B1Agent(B1Agency):
    def __init__(self, setting=None, logger=None, boApp='SAPB1', dataWald=None, boConn=None):
        B1Agency.__init__(self, setting=setting, logger=logger, boApp=boApp, dataWald=dataWald, boConn=boConn)

    def boOrderExtFt(self, o, order):
        pass

    def boOrderLineExtFt(self, i, o, item, order):
        pass
