from setuptools import find_packages, setup

setup(
    name='DataWald-S3Agency',
    version='0.0.2',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='DataWald S3Agency.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='Linux',
    install_requires=['DataWald-Frontend', 'DataWald-BackOffice', 'AWS-S3Connector'],
    classifiers=[
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
