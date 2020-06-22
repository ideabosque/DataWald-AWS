"""
# AWS-S3Connector
=====================

"""
from setuptools import find_packages, setup

setup(
    name='AWS-S3Connector',
    version='0.0.2',
    url='https://github.com/ideabosque/AWS-S3Connector',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect DataWald AWS RESTful API.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['boto3', 'dicttoxml', 'xmltodict'],
    download_url = 'https://github.com/ideabosque/AWS-S3Connector/tarball/0.0.2',
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
