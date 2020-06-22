"""
DataWald-BackOffice
-------------

## Synopsis

Module "datawald_backoffice" is an abstract module that forms the foundation of the data structure and functionalities for backoffice applications.  To apply a specific backoffice application such as financial accounting, shipment process and inventory management etc, the application support module has to extend "datawald_backoffice" module and override couple functions.

## Dependencies

Install the required Python packages by pip.

```bash
pip install -r requirements.txt
```

## Installation

Install the package from the github repository.

```bash
pip install https://github.com/ideabosque/DataWald-Backoffice/tarball/0.0.2
```

## How to extend

The following functions have to be overridden by the extended class/module.

```Python
# Transform order header for the specific backoffice application.
def boOrderFt(self, order):
    return {}

# The extend function for "boOrderft" for customization.
def boOrderExtFt(self, o, order):
    pass

# Transform order line item for the specific backoffice application.
def boOrderLineFt(self, i, o, item, order):
    pass

# The extend function for "boOrderLineFt" for customization.
def boOrderLineExtFt(self, i, o, item, order):
    pass

# The function is used to retrieve the order id from the specific backoffice appliation.
def boOrderIdFt(self, order):
    return None

# Insert an order into the specific backoffice application.
def insertOrderFt(self, o):
    return None
```
"""
from setuptools import find_packages, setup

setup(
    name='DataWald-BackOffice',
    version='0.0.2',
    url='https://bitbucket.org/ideabosque/datawald-backoffice.git',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='DataWald BackOffice.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='Linux',
    install_requires=['DataWald-Abstract', 'AWS-DWConnector'],
    download_url = 'https://github.com/ideabosque/DataWald-Backoffice/tarball/0.0.2',
    classifiers=[
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
