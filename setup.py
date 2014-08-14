#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='simplecoin_rpc_client',
      version='0.1.0',
      author='Isaac Cook',
      author_email='isaac@simpload.com',
      entry_points={
          'console_scripts': [
              'simplecoin_rpc_scheduler = simplecoin_rpc_client.scheduler:entry',
              'simplecoin_rpc = simplecoin_rpc_client.rpc:entry'
          ]
      },
      packages=find_packages()
      )
