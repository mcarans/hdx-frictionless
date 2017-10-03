#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Conversion from HXL to Frictionless

"""
import json
import logging
from collections import OrderedDict
from urllib.parse import urlparse

from datapackage import Package
from hdx.data.resource import Resource
from os.path import basename, splitext

import hxl
from hdx.data.dataset import Dataset
from hdx.hdx_configuration import Configuration
from hxl.io import HXLTagsNotFoundException

logger = logging.getLogger(__name__)


class HXLFrictionlessError(Exception):
    pass


class Converter(object):
    """HXL to Frictionless converter class.

    """
    def __init__(self):
        # type: () -> None
        super(Converter, self).__init__()
        hxl_schema_dataset = Configuration.read()['hxl_schema_dataset']
        dataset = Dataset.read_from_hdx(hxl_schema_dataset)
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
        logger.info('hashtag_schema = %s, attribute schema = %s' % (hashtag_schema, attribute_schema))
        if not hashtag_schema or not attribute_schema:
            raise HXLFrictionlessError('Missing schema!')
        dataset = hxl.data(hashtag_schema)
        self.hashtag_types = dict()
        for row in dataset:
            self.hashtag_types[row.get('#valid_tag')] = row.get('#valid_datatype')

    def convert_hxl_url(self, frictionless_resource):
        dataset = hxl.data(frictionless_resource['path'])
        try:
            dataset.columns
        except HXLTagsNotFoundException:
            return
        frictionless_resource['is_hxl'] = True
        fields = frictionless_resource['schema']['fields']
        for i, header in enumerate(dataset.headers):
            tag = dataset.display_tags[i]
            hashtag = tag.split('+')[0]
            hashtag_type = self.hashtag_types.get(hashtag)
            field = fields[i]
            if hashtag_type is not None:
                if field['type'] != hashtag_type:
                    logger.warning('Overwriting field %s of resource %s\nOld value: %s\nNew value: %s' %
                                   (header, frictionless_resource['name'], field['type'], hashtag_type))
                    field['type'] = hashtag_type
            field['hxl_tag'] = tag

    def convert_hdx_dataset(self, dataset_id, path):
        dataset = Dataset.read_from_hdx(dataset_id)
        package = Package({'id': dataset['id'], 'name': dataset['name'], 'title': dataset['title'],
                           'description': dataset['notes']})
        for hdx_resource in dataset.get_resources():
            package.add_resource({'name': hdx_resource['name'], 'path': hdx_resource['url'],
                                  'format': hdx_resource['format'].lower(), 'title': hdx_resource['description']})
        package.infer()
        for frictionless_resource in package.descriptor['resources']:
            self.convert_hxl_url(frictionless_resource)
        package.commit()
        package.save(path)
