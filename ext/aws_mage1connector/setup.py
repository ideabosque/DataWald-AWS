"""
# AWS-Mage1Connector
=====================

## Synopsis
The python extension is used to connect the Magento 1 with MySQL connection and SOAP api to perform all of the data related functions.

## Configuration

#### MAGE2DBSERVER
Magento 1 MySQL DB Server full DNS name or IP address.

#### MAGE2DBUSERNAME
Magento 1 MySQL username.

#### MAGE2DBPASSWORD
Magento 1 MySQL password.

#### MAGE2DB
Magento 1 MySQL database name.
"""
from setuptools import find_packages, setup

setup(
    name='AWS-Mage1Connector',
    version='0.0.1',
    url='https://github.com/ideabosque/AWS-Mage1Connector',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect Magento 1.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['requests', 'MySQL-python','suds'],
    download_url = 'https://github.com/ideabosque/AWS-Mage1Connector/tarball/0.0.1',
    keywords = ['Magento 1'], # arbitrary keywords
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
