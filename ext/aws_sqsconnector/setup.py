"""
# AWS-SQSConnector
=====================

"""
from setuptools import find_packages, setup

setup(
    name='AWS-SQSConnector',
    version='0.0.2',
    url='https://github.com/ideabosque/AWS-SQSConnector',
    license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='Use to connect DataWald AWS RESTful API.',
    long_description=__doc__,
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
    platforms='any',
    install_requires=['boto3'],
    download_url = 'https://github.com/ideabosque/AWS-SQSConnector/tarball/0.0.2',
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
