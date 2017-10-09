#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import join

from hdx.facades.hdx_scraperwiki import facade

from hdx_frictionless.converter import Converter

logger = logging.getLogger(__name__)


def main():
    """Read HXL schema"""
    converter = Converter()
    converter.convert_hdx_dataset('haiti-population-food-security-outlook-and-livelihood-zones-by-admin-3-for-september-2016', 'haiti_datapackage.json')
    converter.convert_hdx_dataset('lcb-displaced', 'lcb_datapackage.json')
    converter.convert_hdx_dataset('fts-requirements-and-funding-data-for-afghanistan', 'fts_datapackage.json')


if __name__ == '__main__':
    facade(main, hdx_site='prod', hdx_read_only=True, project_config_yaml=join('config', 'project_configuration.yml'))

