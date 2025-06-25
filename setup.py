# -*- coding: utf-8 -*-
#Author: Liuxin YANG
#Date: 2025-06-24

import setuptools

setuptools.setup(
    name='Liuxin packages',
    version='1',
    description='Package for my Dataflow job.',
    packages=setuptools.find_packages(),
    include_package_data=True,
    install_requires=[
        "apache-beam==2.65.0",
        "google-cloud-bigquery==3.34.0",
        "PyYAML==6.0.2",
        "pandas==2.3.0",
        "numpy==2.2.6"
    ]
)

