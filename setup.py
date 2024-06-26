#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-zuora",
    version="1.0.0",
    description="Custom Singer.io tap for extracting data from the Zuora API",
    author="Emmanuvel",
    url="https://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_zuora"],
    install_requires=[
        "singer-python==5.13.0",
        "requests==2.31.0",
        "pendulum==1.2.0",
        "backoff==1.8.0",
        "urllib3==2.2.1"
    ],
    extras_require={"dev": ["ipdb", "pylint"]},
    entry_points="""
          [console_scripts]
          tap-zuora=tap_zuora:main
      """,
    packages=["tap_zuora"],
)
