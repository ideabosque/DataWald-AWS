import traceback
from datetime import datetime
from decimal import Decimal
from time import sleep

class Abstract(object):

    def insertBOEntities(self, entityType, entities, fePrimaryId, boPrimaryId,\
        stack=False, getEntity=None, getEntityId=None, txEntity=None,\
        cancelEntity=None, insertEntities=None, updateEntityStatus=None):
        self.logger.info("Insert entities({0}) into BackOffice.".format(entityType))
        newEntities = []
        entityStatuses = {}
        while len(entities) > 0:
            o = entities.pop()
            entity = getEntity(o["frontend"], o[fePrimaryId])
            id = entity['id']
            entityStatus = {}
            entityStatus["tx_note"] = 'DataWald -> ' + self.boApp
            try:
                boEntityId = getEntityId(entity)
                if boEntityId is None or stack:
                    newEntity = txEntity(entity)
                    newEntity["tx_status"] = 'N'
                    newEntity["tx_note"] = entityStatus["tx_note"]
                    if stack:
                        newEntity[boPrimaryId] = boEntityId
                    newEntities.append(newEntity)
                else:
                    if cancelEntity is not None and entity.get('status', None) == 'canceled':
                        cancelEntity(boEntityId)
                    entityStatus[boPrimaryId] = boEntityId
                    entityStatus["tx_status"] = 'S'
            except Exception as e:
                log = traceback.format_exc()
                entityStatus["tx_status"] = 'F'
                entityStatus["tx_note"] = log
                entityStatus[boPrimaryId] = "####"
                self.logger.exception(e)
            entityStatuses[id] = entityStatus

        if len(newEntities) > 0:
            boEntities = insertEntities(newEntities)
            for entity in boEntities:
                id = entity["id"]
                entityStatuses[id] = {
                    boPrimaryId: entity[boPrimaryId],
                    'tx_status': entity["tx_status"],
                    'tx_note': entity["tx_note"]
                }

        for id, entityStatus in entityStatuses.items():
            try:
                updateEntityStatus(id, entityStatus)
            except:
                log = traceback.format_exc()
                self.logger.exception(log)
        return entityStatuses

    def syncFEEntities(self, entityType, entities, fePrimaryId, boPrimaryId,\
        getEntity=None, syncFt=None, updateEntityStatus=None, validateData=None):
        entitiesStatuses = {}
        while len(entities) > 0:
            e = entities.pop()
            if "data_type" in e.keys():
                entity = getEntity(e["frontend"], e[boPrimaryId], e["data_type"])
            else:
                entity = getEntity(e["frontend"], e[boPrimaryId])
            try:
                if validateData is not None:
                    validateData(entity)
                syncFt(entity)
                entity["tx_status"] = "S"
                entity["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                entity["tx_note"] = 'DataWald -> {0}'.format(self.feApp)
            except Exception as e:
                log = traceback.format_exc()
                entity["tx_status"] = "F"
                entity["tx_dt"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                entity["tx_note"] = log
                entity[fePrimaryId] = "####"
                self.logger.exception(e)

            id = entity["id"]
            entitiesStatuses[id] = {
                fePrimaryId: entity[fePrimaryId],
                'tx_status': entity["tx_status"],
                'tx_note': entity["tx_note"]
            }
            if "data_type" in entity.keys():
                entitiesStatuses[id]["data_type"] = entity["data_type"]

            if entity['tx_status'] == 'F':
                log = "Fail to sync a {0}: {1}/{2}".format(entityType, entity[boPrimaryId], id)
                self.logger.error(log)
                self.logger.error(entity['tx_note'])
            else:
                log = "Successfully sync a {0}: {1}/{2}".format(entityType, entity[boPrimaryId], id)
                self.logger.info(log)

        for id, entityStatus in entitiesStatuses.items():
            try:
                updateEntityStatus(id, entityStatus)
            except:
                log = traceback.format_exc()
                self.logger.exception(log)
            
        return entitiesStatuses

    def transformData(self, record, metadatas, getCustValue=None):
        tgt = {}
        for k, v in metadatas.items():
            try:
                if v["type"] == "list":
                    value = self.extractValue(getCustValue, **self._getParams(record, v["src"][0]))
                    tgt[k] = self.loadData(v["funct"], value, dataType="list", getCustValue=getCustValue)
                elif v["type"] == "dict":
                    value = self.extractValue(getCustValue, **self._getParams(record, v["src"][0]))
                    tgt[k] = self.loadData(v["funct"], value, dataType="dict", getCustValue=getCustValue)
                else:
                    try:
                        src = dict(
                            [(i["label"], self.extractValue(getCustValue, **self._getParams(record, i))) for i in v["src"]]
                        )
                        funct = eval("lambda src: {funct}".format(funct=v["funct"]))
                        tgt[k] = funct(src)
                    except Exception as e:
                        self.logger.info(src)
                        self.logger.info(v["funct"])
                        log = traceback.format_exc()
                        self.logger.exception(log)
                        tgt[k] = None
            except Exception as e:
                log = traceback.format_exc()
                log = "{0}: {1}\nlog: {2}\n".format(k, v, log)
                self.logger.exception(log)
                raise Exception(log)
        tgt = dict([(vkey, vdata) for vkey, vdata in tgt.items() if(vdata is not None)])
        return tgt

    def loadData(self, tx, value, dataType="attribute", getCustValue=None):
        if value is None:
            return None
        elif dataType == "list":
            items = []
            for i in value:
                item = {}
                for k, v in tx.items():
                    item[k] = self.loadData(
                        v if v["type"] == "attribute" else v["funct"],
                        i if v["type"] == "attribute" else self.extractValue(getCustValue, **self._getParams(i, v["src"][0])),
                        dataType=v["type"],
                        getCustValue=getCustValue
                    )
                item = {vkey: vdata for vkey, vdata in item.items() if vdata is not None}
                items.append(item)
            return items
        elif dataType == "dict":
            item = {}
            for k, v in tx.items():
                item[k] = self.loadData(
                    v if v["type"] == "attribute" else v["funct"],
                    value,
                    dataType=v["type"],
                    getCustValue=getCustValue
                )
            item = {vkey: vdata for vkey, vdata in item.items() if vdata is not None}
            return item
        else:
            src = {}
            for i in tx["src"]:
                value = self.extractValue(getCustValue, **self._getParams(value, i))
                if value is not None and isinstance(value, str):
                    src[i["label"]] = value.encode('ascii', 'ignore')
                else:
                    src[i["label"]] = value
            src = {k: v.decode("utf-8", "ignore") if isinstance(v, (bytes, bytearray)) else v for k, v in src.items()}
            funct = lambda src: eval(tx["funct"])
            return funct(src)

    def exists(self, obj, chain):
        _key = chain.pop(0)
        if obj is not None and _key in obj:
            return self.exists(obj[_key], chain) if chain else obj[_key]
        else:
            return None

    def _getParams(self, record, params):
        params["record"] = record
        return params

    def extractValue(self, getCustValue, **params):
        record = params.pop("record")
        key = params.pop("key", None)
        default = params.pop("default", None)
        _value = None
        if key is None:
            pass
        elif key == "####":
            return record
        elif key.find("@") is not -1 and getCustValue is not None:
            _value = getCustValue(record, key)
        else:
            _value = self.exists(record, key.split("|"))
        value = _value if _value is not None else default
        return value

    def updateSyncTask(self, id):
        try:
            syncTask = self.dataWald.getSyncTask(id)
            id = syncTask["id"]
            table = syncTask["table"]
            queues = [
                {
                    'entity': entity,
                    'count': 0
                } for entity in syncTask["entities"]
            ]
            entities = []
            while len(queues):
                queue = queues.pop(0)
                self.logger.info(queue)
                entity = queue['entity']
                taskId = entity["id"]
                queue['count'] = queue['count'] + 1
                if queue['count'] > 1:
                    sleep(2**queue['count']*0.5)
                task = self.dataWald.getTask(table, taskId)
                if task['ready']:
                    entity['task_status'] = task['status']
                    entity['task_detail'] = task['detail']
                    entities.append(entity)
                elif queue['count'] > 6:
                    entity['task_status'] = '?'
                    entity['task_detail'] = 'Not able to retrieve the result from task %s in 8 times.' % taskId
                    entities.append(entity)
                else:
                    queues.append(queue)
            self.dataWald.updateSyncTask(id, entities)
        except Exception as e:
            log = traceback.format_exc()
            self.logger.exception(log)
            raise

        data = self.dataWald.getSyncTask(id)
        log = None
        if data['sync_status'] == "Completed":
            log = "(%s) %s: %s started at %s, completd at %s." % (id, data['frontend'], data['task'], data['start_dt'], data['end_dt'])
        elif data['sync_status'] == "Fail":
            log = "(%s) %s: %s started at %s, Fail!!!" % (id, data['frontend'], data['task'], data['start_dt'])
        elif data['sync_status'] == "Incompleted":
            log = "(%s) %s: %s started at %s, Incompleted!!!" % (id, data['frontend'], data['task'], data['start_dt'])

        if data['sync_status'] == "Completed":
            self.logger.info(log)
        else:
            self.logger.error(log)
            raise Exception(log)
        return data

    def reSync(self, id, funct, key, primary=None, updateDt="update_dt"):
        _syncTask = self.dataWald.getSyncTask(id)
        _entities = list(filter(
            lambda t: ("task_status" not in t.keys() or t["task_status"] not in ("S", "I")),
            _syncTask["entities"]
        ))
        entities = [
            {
                "id": entity["id"],
                key: entity[primary] if primary is not None else entity[key],
                "update_dt":  entity[updateDt]
            } for entity in list(map(lambda x: funct(_syncTask["frontend"], x[key]), _entities))
        ]

        self.dataWald.delSyncTask(id)
        syncTask = {
            "store_code": _syncTask["store_code"],
            "cut_dt": _syncTask["cut_dt"],
            "offset": _syncTask.get("offset", 0),
            "entities": entities
        }
        syncControlId = self.dataWald.insertSyncControl(
            _syncTask["backoffice"],
            _syncTask["frontend"],
            _syncTask["task"],
            _syncTask["table"],
            syncTask
        )
        return {"id": str(syncControlId), "table": _syncTask["table"], "entities": entities}
