from setuptools import find_packages, setup

setup(
    name='DataWald-Frontend',
    version='0.0.2',
    author='Idea Bosque',
    author_email='ideabosque@gmail.com',
    description='DataWald Frontend.',
    long_description=__doc__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    platforms='Linux',
    install_requires=['DataWald-Abstract', 'AWS-DWConnector', 'Cerberus'],
    classifiers=[
        'Programming Language :: Python',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
