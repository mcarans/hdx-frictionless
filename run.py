#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from collections import OrderedDict
from os.path import join

import hxl
from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration

from hdx.facades.hdx_scraperwiki import facade

logger = logging.getLogger(__name__)


class HXLFrictionlessError(Exception):
    pass


def main():
    """Read HXL schema"""
    hxl_schema_dataset = Configuration.read()['hxl_schema_dataset']
    dataset = Dataset.read_from_hdx(hxl_schema_dataset) # type: Dataset
    if dataset is None:
        raise HXLFrictionlessError('HXL Schema Dataset: %s does not exist!' % hxl_schema_dataset)
    resources = dataset.get_resources()
    hashtag_schema = None
    attribute_schema = None
    for resource in resources:
        if 'hashtag' in resource['name']:
            hashtag_schema = resource['url']
        elif 'attribute' in resource['name']:
            attribute_schema = resource['url']
    logger.info('hashtag_schema = %s, attribute schema = %s')
    if not hashtag_schema or not attribute_schema:
        raise HXLFrictionlessError('Missing schema!')
    dataset = hxl.data(hashtag_schema)
    hashtag_types = dict()
    for row in dataset:
        hashtag_types[row.get('#valid_tag')] = row.get('#valid_datatype')

    file = 'fts_funding_requirements_afg.csv'
    dataset = hxl.data(file, allow_local=True)
    print(dataset.headers)
    print(dataset.display_tags)
    jsonoutput = OrderedDict()
    jsonoutput['name'] = file
    jsonoutput['datapackage_version'] = '1.0-beta'
    jsonoutput['title'] = 'Afghanistan - Requirements and Funding Data'
    resource = OrderedDict()
    jsonoutput['resources'] = [resource]
    resource['url'] = 'https://data.humdata.org/dataset/6a60da4e-253f-474f-8683-7c9ed9a20bf9/resource/96cc8cda-120f-483a-a7ce-5f89af4d99ee/download/fts_funding_requirements_afg.csv'
    fields = list()
    resource['schema'] = {'fields': fields}
    for i, header in enumerate(dataset.headers):
        tag = dataset.display_tags[i]
        hashtag = tag.split('+')[0]
        hashtag_type = hashtag_types.get(hashtag)
        field = OrderedDict([('name', header)])
        if hashtag_type is not None:
            field['type'] = hashtag_type
        fields.append(field)
    print(jsonoutput)


if __name__ == '__main__':
    facade(main, hdx_site='prod', hdx_read_only=True, project_config_yaml=join('config', 'project_configuration.yml'))

