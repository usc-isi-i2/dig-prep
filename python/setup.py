# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='dig-extract',
    version='0.0.1',
    description='Dig-Extract module for Karma Dig',
    long_description=readme,
    author='Andrew Philpot',
    author_email='andrew.philpot@gmail.com',
    url='https://github.com/InformationIntegrationGroup/dig-extract',
    license=license,
    packages=find_packages(exclude=('tests', 'docs')),
    # package_data={'dig': ['extract/entity/phone/areacode.json',
    #                       'extract/page/market.json']
    #               }
    )
