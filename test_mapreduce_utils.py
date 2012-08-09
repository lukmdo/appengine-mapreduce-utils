import os
import sys
import operator
import math

import unittest2 as unittest
from google.appengine.ext import db
from google.appengine.ext import testbed
from mapreduce import model

from mapreduce_utils import DatastoreQueryInputReader
from mapreduce_utils import KeyRange


class TestEntity(db.Model):
    name = db.StringProperty()
    value = db.StringProperty()
    type = db.StringProperty(choices=['A', 'B', 'C'])


class DatastoreQueryInputReaderTest(unittest.TestCase):
    _old_sys_path = sys.path
    _tests_dir_path = sys.path.insert(0, os.path.dirname(__file__))

    @staticmethod
    def simple_parametrized_filter_factory(type, *args, **kwargs):
        return lambda item, type=type: item.type == type

    def setUp(self):
        sys.path = self._tests_dir_path

        self.TestEntity = TestEntity
        self.TEST_ENTITY_IMPORT_PATH = "test_mapreduce_utils.TestEntity"

        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()
        self.dataSet = (
            # type A
            dict(type="A", name="name1", value="value1"),

            # type B
            dict(type="B", name="name1", value="value1"),
            dict(type="B", name="name2", value="value2"),

            # type C
            dict(type="C", name="name1", value="value1"),
            dict(type="C", name="name2", value="value2"),
            dict(type="C", name="name3", value="value3"),
        )

        db.put([self.TestEntity(**data) for data in self.dataSet])

    def tearDown(self):
        self.testbed.deactivate()
        sys.path = self._old_sys_path

    def test_world(self):
        SHARD_COUNT = 10

        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            {
                "input_reader": {
                    "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                }
            },
            SHARD_COUNT)

        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        got = reduce(operator.add,
            (list(reader) for reader in ds_input_readers))
        self.assertEqual(len(self.dataSet), len(got))

    def test_split_input(self):
        SHARD_COUNT = 10
        BATCH_SIZE = 2
        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            {
                "input_reader": {
                    "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                    "batch_size": BATCH_SIZE,
                }
            },
            SHARD_COUNT)

        def num_expected():
            batch_size = min(len(self.dataSet), BATCH_SIZE)
            free_division = abs(len(self.dataSet) / batch_size)
            return min(free_division, SHARD_COUNT)

        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        self.assertEqual(3, num_expected())  # 1-3, 3-5, 5-None
        self.assertEqual(3, len(ds_input_readers))

        # batch_size = dataSet bigger half
        BATCH_SIZE = int(math.ceil(len(self.dataSet) / 2.0))
        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            {
                "input_reader": {
                    "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                    "batch_size": BATCH_SIZE,
                }
            },
            SHARD_COUNT)
        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        self.assertEqual(2, num_expected())  # 1-4, 4-None
        self.assertEqual(2, len(ds_input_readers))

        # batch_size > dataSet itself
        BATCH_SIZE = len(self.dataSet) * 2
        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            {
                "input_reader": {
                    "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                    "batch_size": BATCH_SIZE,
                }
            },
            SHARD_COUNT)
        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        self.assertEqual(1, num_expected())  # 1-None
        self.assertEqual(1, len(ds_input_readers))

    def test_with_query_filters(self):
        SHARD_COUNT = 10
        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            {
                "input_reader": {
                    "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                    "filters": [("type", "=", "C")],
                }
            },
            SHARD_COUNT)

        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        got = reduce(operator.add,
            (list(reader) for reader in ds_input_readers))
        self.assertEqual(3, len(got))
        data1, data2, data3 = filter(lambda i: i['type'] == "C", self.dataSet)
        got.sort(key=lambda i: i.name)
        self.assertDictEqual(data1, db.to_dict(got.pop(0)))
        self.assertDictEqual(data2, db.to_dict(got.pop(0)))
        self.assertDictEqual(data3, db.to_dict(got.pop(0)))

    def test_with_filter_factory(self):
        SHARD_COUNT = 10
        FF_PATH = \
            "test_mapreduce_utils.DatastoreQueryInputReaderTest." \
            "simple_parametrized_filter_factory"

        params = {
            "input_reader": {
                "entity_kind": self.TEST_ENTITY_IMPORT_PATH,
                "filter_factory_spec": {
                    "name": FF_PATH,
                    "args": ["B"]
                }
            }
        }
        mapper_spec = model.MapperSpec(
            "FooHandler",
            "mapreduce_utils.DatastoreQueryInputReader",
            params,
            SHARD_COUNT)

        ds_input_readers = DatastoreQueryInputReader.split_input(mapper_spec)
        got = reduce(operator.add,
            (list(reader) for reader in ds_input_readers))
        self.assertEqual(2, len(got))
        data1, data2, = filter(lambda i: i['type'] == "B", self.dataSet)
        got.sort(key=lambda i: i.name)
        self.assertDictEqual(data1, db.to_dict(got.pop(0)))
        self.assertDictEqual(data2, db.to_dict(got.pop(0)))


class KeyRangeTest(unittest.TestCase):
    class TestEntity(db.Model):
        name = db.StringProperty()
        value = db.StringProperty()

    def setUp(self):
        self.testbed = testbed.Testbed()
        self.testbed.activate()
        self.testbed.init_datastore_v3_stub()

        self.dataSet = (
            dict(name='name1', value='val1'),
            dict(name='name2', value='val2'),
            dict(name='name3', value='val3'))
        db.put([self.TestEntity(**data) for data in self.dataSet])

    def tearDown(self):
        self.testbed.deactivate()

    def test_world(self):
        """foo test"""
        query = KeyRange().make_query(self.TestEntity)
        self.assertEqual(3, len(list(query)))

    def test_filters(self):
        data1, data2, data3 = self.dataSet[:3]

        query = KeyRange().make_ascending_query(
            self.TestEntity,
            filters=[("name", "=", data1["name"])])
        got = list(query)
        self.assertEqual(1, len(got))
        self.assertDictEqual(data1, db.to_dict(got.pop()))

        query = KeyRange().make_query(
            self.TestEntity,
            filters=[("name", "IN", [data1["name"], data2["name"]])],
            order=['name'])
        got = list(query)
        self.assertEqual(2, len(got))
        self.assertDictEqual(data1, db.to_dict(got.pop(0)))
        self.assertDictEqual(data2, db.to_dict(got.pop(0)))
