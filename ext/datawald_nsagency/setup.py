"""
DataWald-NSAgency
-------------

"""
from setuptools import find_packages, setup

setup(
    name='DataWald-NSAgency',
    version='0.0.2',
    url='https://bitbucket.org/ideabosque/datawald-nsagency.git',
    #license='MIT',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='DataWald NSAgency.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='Linux',
    install_requires=['DataWald-BackOffice', 'AWS-NSConnector'],
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
