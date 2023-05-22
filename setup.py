# -*- coding: utf-8 -*-
"""
Created on Wed Nov  3 14:09:18 2021

"""

from setuptools import find_packages
from setuptools import setup

import os

# loc = os.path.dirname(os.path.realpath(__file__))
# requirementPath = loc + '/requirements.txt'
# install_requires = []

# if os.path.isfile(requirementPath):
    # with open(requirementPath) as f:
        # install_requires = f.read().splitlines()
    
setup(
          name="pylizard", 
          version='1.0.1',
          description='pylizard',
          author='Vitens',
          author_email='vitens@outlook.com',

          packages=find_packages(exclude=['tests','examples']),
          install_requires=['requests>=2.24.0','lxml>=4.6.1','uuid'],
          keywords=['python',],
          classifiers= [    
             "Programming Language :: Python :: 3",
             "Operating System :: Microsoft :: Windows",
          ]                 
          )