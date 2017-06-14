#!/usr/bin/env python

from setuptools import setup

requirements = [
    'click',
    'boto3',
    'kinesis_producer'
]

setup(
    name="restream",
    version='0.1',
    py_modules=['restream'],
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'restream=restream:restream'
        ]
    }
)
