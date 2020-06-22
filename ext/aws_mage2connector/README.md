# AWS-Mage2Connector
=====================

## Synopsis
The python extension is used to connect the Magento 2 with MySQL connection and RESTful API to perform all of the data related functions.

## Configuration

```python
import logging, sys
logging.basicConfig(
	level=logging.INFO
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger()

from aws_mage2connector import Mage2Connector
setting = {
    "MAGE2DBSERVER": "127.0.0.1",
	"MAGE2DBUSERNAME": "admin",
	"MAGE2DBPASSWORD": "12345abc",
	"MAGE2DB": "mage23ee",
	"MAGE2DBPORT": 10022,
	"VERSION": "EE"
}
mage2Connector = Mage2Connector(setting=setting, logger=logger)
```

#### MAGE2DBSERVER
Magento 2 MySQL DB Server full DNS name or IP address.

#### MAGE2DBUSERNAME
Magento 2 MySQL username.

#### MAGE2DBPASSWORD
Magento 2 MySQL password.

#### MAGE2DB
Magento 2 MySQL database name.

#### MAGE2DBPORT
Magento 2 MySQL database port.

#### VERSION
Magento 2 EE/CE

## Sync Products
```python
sku='abc-111'
attributeSet='Default'
data={
	'status': '1',
	'visibility': '4',
	'tax_class_id': '2',
	'short_description': 'XXXXXXXXXX', 
	'color': 'White', 
	'weight': '1',
	'name': 'XXXXXXXXXX', 
	'description': 'XXXXXXXXXX', 
	'price': '5.0'
}
typeId='simple'
storeId='0'
mage2Connector.syncProduct(sku, attributeSet, data, typeId, storeId)
```

## Related Articles

  * [Google Sheets as PIM for Magento 2](https://ideabosque.github.io/magento2/googlesheets/2019/12/08/Google-Sheets-as-PIM-for-Magento-2/)

Feel free to [create a GitHub issue](https://github.com/ideabosque/AWS-Mage2Connector/issues/new) or [send us an email](mailto:ideabosque@gmail.com) for support regarding this application.
