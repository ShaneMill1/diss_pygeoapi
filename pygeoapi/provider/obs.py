# =================================================================
#
# Authors: Shane Mill <shane.mill@noaa.gov>
#
# Copyright (c) 2023 Shane Mill
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

from typing import Dict
from collections import OrderedDict
import json
import logging
import uuid
import os
import boto3
from botocore.client import Config

from elasticsearch import Elasticsearch, exceptions, helpers
from elasticsearch_dsl import Search, Q

from pygeoapi.provider.base import (BaseProvider, ProviderConnectionError,
                                    ProviderQueryError,
                                    ProviderItemNotFoundError)
from pygeoapi.models.cql import CQLModel, get_next_node
from pygeoapi.util import get_envelope, crs_transform
import io

LOGGER = logging.getLogger(__name__)

class MetarProvider(BaseProvider):
    """Elasticsearch Provider"""

    def __init__(self, provider_def):
        """
        Initialize object

        :param provider_def: provider definition

        :returns: pygeoapi.provider.elasticsearch_.ElasticsearchProvider
        """

        super().__init__(provider_def)

        self.select_properties = []

        self.es_host, self.index_name = self.data.rsplit('/', 1)

        LOGGER.debug('Setting Elasticsearch properties')

        LOGGER.debug(f'host: {self.es_host}')
        LOGGER.debug(f'index: {self.index_name}')

        LOGGER.debug('Connecting to Elasticsearch')
        self.es = Elasticsearch(self.es_host)
        if not self.es.ping():
            msg = f'Cannot connect to Elasticsearch: {self.es_host}'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        LOGGER.debug('Determining ES version')
        v = self.es.info()['version']['number'][:3]
        if float(v) < 8:
            msg = 'only ES 8+ supported'
            LOGGER.error(msg)
            raise ProviderConnectionError(msg)

        LOGGER.debug('Grabbing field information')
        self.provider_def=provider_def
        try:
            self.fields = self.get_fields()
        except exceptions.NotFoundError as err:
            LOGGER.error(err)
            raise ProviderQueryError(err)

    def mask_prop(self, property_name):
        """
        generate property name based on ES backend setup

        :param property_name: property name

        :returns: masked property name
        """

        return f'properties.{property_name}'


    def get_fields(self):
        """
         Get provider field information (names, types)

        :returns: dict of fields
        """

        fields_ = {}
        ii = self.es.indices.get(index=self.index_name, allow_no_indices=False)

        LOGGER.debug(f'Response: {ii}')
        try:
            if '*' not in self.index_name:
                p = ii[self.index_name]['mappings']['properties']['properties']
            else:
                LOGGER.debug('Wildcard index; setting from first match')
                index_name_ = list(ii.keys())[0]
                p = ii[index_name_]['mappings']['properties']['properties']
        except KeyError:
            LOGGER.warning('Trying for alias')
            alias_name = next(iter(ii))
            p = ii[alias_name]['mappings']['properties']['properties']
        except IndexError:
            LOGGER.warning('could not get fields; returning empty set')
            return {}

        for k, v in p['properties'].items():
            if 'type' in v:
                if v['type'] == 'text':
                    fields_[k] = {'type': 'string'}
                elif v['type'] == 'date':
                    fields_[k] = {'type': 'string', 'format': 'date'}
                elif v['type'] in ('float', 'long'):
                    fields_[k] = {'type': 'number', 'format': v['type']}
                else:
                    fields_[k] = {'type': v['type']}

        return fields_

    def get(self, identifier, **kwargs):

        s3 = boto3.resource('s3',endpoint_url=self.provider_def['minio_server'],aws_access_key_id=self.provider_def['minio_user'],aws_secret_access_key=self.provider_def['minio_password'],verify=False)
        minio_path=self.provider_def['minio_path']
        year=identifier.split('_')[2][0:4];month=identifier.split('_')[2][4:6];day=identifier.split('_')[2][6:8]
        date=year+'-'+month+'-'+day
        minio_path=minio_path.replace('<DATE>',date)
        obj = s3.Object(self.provider_def['minio_bucket'], minio_path+'/'+identifier.split('-')[0]+'.bufr4')
        response=obj.get()['Body'].read()
        return response

    def query(self, offset=0, limit=10, resulttype='results',
              bbox=[], datetime_=None, properties=[], sortby=[],
              select_properties=[], skip_geometry=False, q=None,
              filterq=None, **kwargs):
        """
        query Elasticsearch index

        :param offset: starting record to return (default 0)
        :param limit: number of records to return (default 10)
        :param resulttype: return results or hit limit (default results)
        :param bbox: bounding box [minx,miny,maxx,maxy]
        :param datetime_: temporal (datestamp or extent)
        :param properties: list of tuples (name, value)
        :param sortby: list of dicts (property, order)
        :param select_properties: list of property names
        :param skip_geometry: bool of whether to skip geometry (default False)
        :param q: full-text search term(s)
        :param filterq: filter object

        :returns: dict of 0..n GeoJSON features
        """

        self.select_properties = select_properties

        query = {'track_total_hits': True, 'query': {'bool': {'filter': []}}}
        filter_ = []

        feature_collection = {
            'type': 'FeatureCollection',
            'features': []
        }

        if resulttype == 'hits':
            LOGGER.debug('hits only specified')
            limit = 0

        if bbox:
            LOGGER.debug('processing bbox parameter')
            minx, miny, maxx, maxy = bbox
            bbox_filter = {
                'geo_shape': {
                    'geometry': {
                        'shape': {
                            'type': 'envelope',
                            'coordinates': [[minx, maxy], [maxx, miny]]
                        },
                        'relation': 'intersects'
                    }
                }
            }

            query['query']['bool']['filter'].append(bbox_filter)

        if datetime_ is not None:
            LOGGER.debug('processing datetime parameter')
            if self.time_field is None:
                LOGGER.error('time_field not enabled for collection')
                raise ProviderQueryError()

            time_field = self.mask_prop(self.time_field)

            if '/' in datetime_:  # envelope
                LOGGER.debug('detected time range')
                time_begin, time_end = datetime_.split('/')

                range_ = {
                    'range': {
                        time_field: {
                            'gte': time_begin,
                            'lte': time_end
                        }
                    }
                }
                if time_begin == '..':
                    range_['range'][time_field].pop('gte')
                elif time_end == '..':
                    range_['range'][time_field].pop('lte')

                filter_.append(range_)

            else:  # time instant
                LOGGER.debug('detected time instant')
                filter_.append({'match': {time_field: datetime_}})

            LOGGER.debug(filter_)
            query['query']['bool']['filter'].append(*filter_)

        if properties:
            LOGGER.debug('processing properties')
            for prop in properties:
                prop_name = self.mask_prop(prop[0])
                pf = {
                    'match': {
                        prop_name: {
                            'query': prop[1]
                        }
                    }
                }
                query['query']['bool']['filter'].append(pf)

            if '|' not in prop[1]:
                pf['match'][prop_name]['minimum_should_match'] = '100%'

        if sortby:
            LOGGER.debug('processing sortby')
            query['sort'] = []
            for sort in sortby:
                LOGGER.debug(f'processing sort object: {sort}')

                sp = sort['property']

                if (self.fields[sp]['type'] == 'string'
                        and self.fields[sp].get('format') != 'date'):
                    LOGGER.debug('setting ES .raw on property')
                    sort_property = f'{self.mask_prop(sp)}.raw'
                else:
                    sort_property = self.mask_prop(sp)

                sort_order = 'asc'
                if sort['order'] == '-':
                    sort_order = 'desc'

                sort_ = {
                    sort_property: {
                        'order': sort_order
                    }
                }
                query['sort'].append(sort_)

        if q is not None:
            LOGGER.debug('Adding free-text search')
            query['query']['bool']['must'] = {'query_string': {'query': q}}

            query['_source'] = {
                'excludes': [
                    'properties._metadata-payload',
                    'properties._metadata-schema',
                    'properties._metadata-format'
                ]
            }

        if self.properties or self.select_properties:
            LOGGER.debug('filtering properties')

            all_properties = self.get_properties()

            query['_source'] = {
                'includes': list(map(self.mask_prop, all_properties))
            }

            query['_source']['includes'].append('id')
            query['_source']['includes'].append('type')
            query['_source']['includes'].append('geometry')

        if skip_geometry:
            LOGGER.debug('excluding geometry')
            try:
                query['_source']['excludes'] = ['geometry']
            except KeyError:
                query['_source'] = {'excludes': ['geometry']}
        try:
            LOGGER.debug('querying Elasticsearch')
            if filterq:
                # LOGGER.debug(f'adding cql object: {filterq.model_dump_json()}')
                query = update_query(input_query=query, cql=filterq)
            LOGGER.debug(json.dumps(query, indent=4))

            LOGGER.debug('Testing for ES scrolling')
            if offset + limit > 10000:
                gen = helpers.scan(client=self.es, query=query,
                                   preserve_order=True,
                                   index=self.index_name)
                results = {'hits': {'total': limit, 'hits': []}}
                for i in range(offset + limit):
                    try:
                        if i >= offset:
                            results['hits']['hits'].append(next(gen))
                        else:
                            next(gen)
                    except StopIteration:
                        break

                matched = len(results['hits']['hits']) + offset
                returned = len(results['hits']['hits'])
            else:
                es_results = self.es.search(index=self.index_name,
                                            from_=offset, size=limit, **query)
                results = es_results
                matched = es_results['hits']['total']['value']
                returned = len(es_results['hits']['hits'])

        except exceptions.ConnectionError as err:
            LOGGER.error(err)
            raise ProviderConnectionError()
        except exceptions.RequestError as err:
            LOGGER.error(err)
            raise ProviderQueryError()
        except exceptions.NotFoundError as err:
            LOGGER.error(err)
            raise ProviderQueryError()

        feature_collection['numberMatched'] = matched

        if resulttype == 'hits':
            return feature_collection

        feature_collection['numberReturned'] = returned

        LOGGER.debug('serializing features')
        for feature in results['hits']['hits']:
            feature_ = self.esdoc2geojson(feature)
            feature_collection['features'].append(feature_)

        return feature_collection

    def esdoc2geojson(self, doc):
        """
        generate GeoJSON `dict` from ES document

        :param doc: `dict` of ES document

        :returns: GeoJSON `dict`
        """

        feature_ = {}
        feature_thinned = {}

        LOGGER.debug('Fetching id and geometry from GeoJSON document')
        feature_ = doc['_source']

        if self.id_field in doc['_source']['properties']:
            id_ = doc['_source']['properties'][self.id_field]
        else:
            id_ = doc['_source'].get('id', doc['_id'])

        feature_['id'] = id_
        feature_['geometry'] = doc['_source'].get('geometry')

        if self.properties or self.select_properties:
            LOGGER.debug('Filtering properties')
            all_properties = self.get_properties()

            feature_thinned = {
                'id': id_,
                'type': feature_['type'],
                'geometry': feature_.get('geometry'),
                'properties': OrderedDict()
            }
            for p in all_properties:
                try:
                    feature_thinned['properties'][p] = feature_['properties'][p]  # noqa
                except KeyError as err:
                    LOGGER.error(err)
                    raise ProviderQueryError()

        if feature_thinned:
            return feature_thinned
        else:
            return feature_





