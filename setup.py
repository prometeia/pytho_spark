# -*- coding: utf-8 -*-
from promebuilder import gen_metadata, setup
from setuptools import find_packages

METADATA = gen_metadata(
    name="pytho_spark",
    description="Pytho-Spark package, Pytho to Spark integration via Oozie REST API",
    email="pytho_support@prometeia.com",
    keywords="multikernel pytho spark",
    url="https://github.com/prometeia/pytho_spark"
)

if __name__ == '__main__':
    setup(METADATA)
