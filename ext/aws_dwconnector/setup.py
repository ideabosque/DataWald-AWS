"""
# AWS-DWConnector
=====================

## Synopsis
The python extension is used to connect the DataWald AWS with RESTful api to perform all of the data related functions.

## Configuration

#### DWRESTUSR
DataWald RESTful api username.

#### DWRESTPASS
DataWald RESTful api password.

#### DWRESTENDPOINT
DataWald RESTful api endpoint url.
"""
from setuptools import find_packages, setup

setup(
    name='AWS-DWConnector',
    version='0.0.2',
    url='https://github.com/ideabosque/AWS-DWConnector',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect DataWald AWS RESTful API.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['requests',],
    download_url = 'https://github.com/ideabosque/AWS-DWConnector/tarball/0.0.2',
    keywords = ['DataWald'], # arbitrary keywords
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
