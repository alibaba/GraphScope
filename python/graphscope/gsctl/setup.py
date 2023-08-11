import io
import os
import subprocess

import click

from setuptools import setup

setup(
    name='gsctl',
    version='1.0',
    packages=['.'],
    install_requires=[
        
    ],
    entry_points={
        'console_scripts': [
            'gsctl=main:cli',  # 可执行脚本的入口
        ],
    },
)