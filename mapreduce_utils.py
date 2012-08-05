"""MapReduce utilities that extend mapreduce library

https://developers.google.com/appengine/docs/python/dataprocessing/
"""
from collections import defaultdict
from mapreduce import namespace_range

try:
    import json as simplejson
except ImportError:
    from mapreduce.lib import simplejson

import itertools
from mapreduce.lib import key_range
from google.appengine.ext import db
from google.appengine.ext import ndb
from mapreduce.input_readers import AbstractDatastoreInputReader
from mapreduce.input_readers import _get_params
import mapreduce.operation
import mapreduce.util
import py5to7


class KeyRange(key_range.KeyRange):
    """Extended mapreduce.lib.key_range.KeyRange

    Lets you pass string serialized db.Key filter val.

    Main idea: make it more flexible by extending base class
    Use case: Enables constructing query from serialized query params
    """

    def make_query(self, kind_class, keys_only=False, filters=None,
                   order=None):
        """Return a db.Query from provided params"""
        if ndb is not None:
          if issubclass(kind_class, ndb.Model):
            return self.make_ascending_ndb_query(
                kind_class, keys_only=keys_only, filters=filters)
        assert self._app is None, '_app is not supported for db.Query'
        query = db.Query(kind_class, namespace=self.namespace,
                         keys_only=keys_only)

        query = self.filter_query(query, filters=filters)
        if order:
            for item in order:
                query.order(item)
        return query

    def make_ascending_query(self, kind_class, keys_only=False, filters=None):
        return self.make_query(
            kind_class,
            keys_only=keys_only,
            filters=filters,
            order=['__key__'])

    def filter_query(self, query, filters=None):
        kind = query._model_class

        if filters:
            for k, c, v in filters:
                kind_property = getattr(kind, k)
                if isinstance(kind_property, db.ReferenceProperty) and \
                   isinstance(v, basestring):
                    v = db.Key(v)
                query.filter("%s %s" % (k, c), v)

        if self.key_start:
            constrain = "__key__ >=" if self.include_start else "__key__ >"
            query.filter(constrain, self.key_start)
        if self.key_end:
            constrain = "__key__ <=" if self.include_end else "__key__ <"
            query.filter(constrain, self.key_end)
        return query

    @classmethod
    def from_json(cls, json_str):

        """mapreduce.lib.KeyRange.from json is staticmethod
        with harcoded class...

        Deserialize KeyRange from its json representation.

        Args:
          json_str: string with json representation created by key_range_to_json.

        Returns:
          deserialized KeyRange instance.
        """
        def key_from_str(key_str):
            if key_str:
                return db.Key(key_str)
            else:
                return None

        json = simplejson.loads(json_str)
        return cls(key_from_str(json["key_start"]),
                        key_from_str(json["key_end"]),
                        json["direction"],
                        json["include_start"],
                        json["include_end"],
                        json.get("namespace"),
                        _app=json.get("_app"))


class DatastoreQueryInputReader(AbstractDatastoreInputReader):
    """DatastoreInputReader yields data from query_params baked query

    It is functional extension of mapreduce.input_readers.DatastoreInputReader.
    It narrows the iter yield data to query results before they reach mappers.
    """

    PRE_MAP_FILTER_PARAM = "pre_map_filter"
    PRE_MAP_FF_SPEC_PARAM = "filter_factory_spec"

    @classmethod
    def validate(cls, mapper_spec):
        pass

    @classmethod
    def inject_ff(cls, obj, ff_spec):
        """Injects pre_map_filter to object based on ff_spec

        @TODO: update and review this doc!
        Returns object for convenience. The idea of ``filter_factory``
        is similar as ``DatastoreInputReader.filters``. The difference is that
        you will be willing to use ``filter_factory`` when the you hit GQL
        query or your model limitations.

        ``filter_factory`` will jump in before mapping stage
        (even before splitting stage) and pasing on the value returned from
        factory produced function will pass/hold the datastore object.

            :param filter_factory['name']: import path ie. 'module.func_name'
            :param filter_factory['args']: optional params list
            :param filter_factory['kwargs']: optional params dict
        """
        if not ff_spec:
            return obj

        ff_name = ff_spec['name']
        ff_args = ff_spec.get('args', [])
        ff_kwargs = ff_spec.get('kwargs', {})

        ff = mapreduce.util.for_name(ff_name)
        ff_product = ff(*ff_args, **ff_kwargs)
        setattr(obj, cls.PRE_MAP_FF_SPEC_PARAM, ff_spec)
        setattr(obj, cls.PRE_MAP_FILTER_PARAM, ff_product)

    def __init__(self, *args, **kwargs):
        ff_spec = kwargs.pop(self.PRE_MAP_FF_SPEC_PARAM, None)
        super(DatastoreQueryInputReader, self).__init__(*args, **kwargs)

        if ff_spec:
            self.__class__.inject_ff(self, ff_spec)

    def to_json(self):
        data = super(DatastoreQueryInputReader, self).to_json()
        ff_spec = getattr(self, self.PRE_MAP_FF_SPEC_PARAM, None)
        if ff_spec:
            data[self.PRE_MAP_FF_SPEC_PARAM] = ff_spec
        return data

    @classmethod
    def from_json(cls, json):
        """mapreduce.KeyRange class was hardcoded ...."""
        if json[cls.KEY_RANGE_PARAM] is None:
            key_ranges = None
        else:
            key_ranges = []
            for k in json[cls.KEY_RANGE_PARAM]:
                if k:
                    key_ranges.append(KeyRange.from_json(k))
                else:
                    key_ranges.append(None)

        if json[cls.NAMESPACE_RANGE_PARAM] is None:
            ns_range = None
        else:
            ns_range = namespace_range.NamespaceRange.from_json_object(
                json[cls.NAMESPACE_RANGE_PARAM])

        if json[cls.CURRENT_KEY_RANGE_PARAM] is None:
            current_key_range = None
        else:
            current_key_range = key_range.KeyRange.from_json(
                json[cls.CURRENT_KEY_RANGE_PARAM])

        return cls(
            json[cls.ENTITY_KIND_PARAM],
            key_ranges,
            ns_range,
            json[cls.BATCH_SIZE_PARAM],
            current_key_range,
            filters=json.get(cls.FILTERS_PARAM),
            filter_factory_spec=json.get(cls.PRE_MAP_FF_SPEC_PARAM))

    @classmethod
    def split_input(cls, mapper_spec):
        params = _get_params(mapper_spec)
        entity_kind_name = params.pop(cls.ENTITY_KIND_PARAM)
        batch_size = int(params.get(cls.BATCH_SIZE_PARAM, cls._BATCH_SIZE))
        filters = params.get(cls.FILTERS_PARAM)
        shard_count = mapper_spec.shard_count

        ff_spec = params.get(cls.PRE_MAP_FF_SPEC_PARAM)
        ### init params

        entity_kind = mapreduce.util.for_name(entity_kind_name)
        splitter_query = KeyRange().make_ascending_query(
            entity_kind, keys_only=True, filters=filters)

        key_begin_iter = itertools.islice(splitter_query, None, None,
                                          batch_size)
        key_ranges = defaultdict(list)
        key_begin = py5to7.next(key_begin_iter, None)
        for (i, key) in enumerate(key_begin_iter):
            key_ranges[i % shard_count].append(
                KeyRange(key_begin, key, include_end=False))
            key_begin = py5to7.next(key_begin_iter, None)

        if key_begin and not key_ranges:
            key_ranges[0] = [KeyRange(key_begin, None, include_end=False)]

        return [cls(entity_kind_name,
                    key_ranges=k,
                    batch_size=batch_size,
                    filters=filters,
                    filter_factory_spec=ff_spec) for k in key_ranges.values()]

    def __iter__(self):
        entity_kind = mapreduce.util.for_name(self._entity_kind)
        filters = self._filters

        for k_range in self._key_ranges:
            query = k_range.make_ascending_query(entity_kind, filters=filters)
            if not isinstance(query, db.Query):
                return self._iter_ndb(query)

            pre_map_filter = getattr(self, self.PRE_MAP_FILTER_PARAM, None)
            if pre_map_filter:
                query = itertools.ifilter(pre_map_filter, query)
            return iter(query)

    def _iter_ndb(self, query):
        cursor = None
        while True:
            results, cursor, more = query.fetch_page(self._batch_size,
                                                     start_cursor=cursor)
            for model_instance in results:
                key = model_instance.key
                yield key, model_instance
            if not more:
                break
