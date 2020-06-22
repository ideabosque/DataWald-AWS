"""
AWS-BCConnector
-------------

"""
from setuptools import find_packages, setup

setup(
    name='AWS-BCConnector',
    version='0.0.1',
    url='https://bitbucket.org/ideabosque/aws-bcconnector.git',
    #license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='AWS BCConnector.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='Linux',
    install_requires=[],
    classifiers=[
        #'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
