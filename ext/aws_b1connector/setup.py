"""
AWS-B1Connector
-------------

## Synopsis
The python extension is used to connect [SAP B1 RESTful](https://github.com/ideabosque/SAP-B1-RESTful) api to perform all of the data related functions.

## Configuration

#### B1RESTUSR
[SAP B1 RESTful](https://github.com/ideabosque/SAP-B1-RESTful) api username.

#### B1RESTPASS
[SAP B1 RESTful](https://github.com/ideabosque/SAP-B1-RESTful) api password.

#### B1RESTENDPOINT
[SAP B1 RESTful](https://github.com/ideabosque/SAP-B1-RESTful) api endpoint url.

"""

from setuptools import find_packages, setup

setup(
    name='AWS-B1Connector',
    version='0.0.1',
    url='https://github.com/ideabosque/AWS-B1Connector',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect SAP B1 RESTful API.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['requests',],
    download_url = 'https://github.com/ideabosque/AWS-B1Connector/tarball/0.0.1',
    keywords = ['SAP B1', 'SAP Business One', 'DataWald',], # arbitrary keywords
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
