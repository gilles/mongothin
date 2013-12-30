# coding=utf-8
import unittest

from bson import ObjectId

import mongothin.connection
from mongothin.resource import Resource, MissingIdsException


class MongoResource(Resource):
    _collection = 'argh'


class TestResource(unittest.TestCase):
    def setUp(self):
        """Setup

        """
        super(TestResource, self).setUp()
        mongothin.connection.register_connection('default', 'mongothin')
        self.client = mongothin.connection.get_connection('default')

    def tearDown(self):
        """Teardown

        """
        super(TestResource, self).tearDown()
        self.client.drop_database('mongothin')

    def test_insert(self):
        object_id = MongoResource.insert({'test': 'test'})
        result = MongoResource.find_one(object_id)
        self.assertDictEqual(result, {'test': 'test', '_id': object_id})

    def test_remove(self):
        object_id = MongoResource.insert({'test': 'test'})
        ret = MongoResource.remove(object_id)
        self.assertEqual(ret, 1)
        ret = MongoResource.find_one(object_id)
        self.assertIsNone(ret)

    def test_update(self):
        object_id = MongoResource.insert({'test': 'test'})
        MongoResource.update(object_id, {'$inc': {'blah': 1}})
        result = MongoResource.find_one(object_id)
        self.assertDictEqual(result, {'test': 'test', 'blah': 1, '_id': object_id})

    def test_update_dict(self):
        object_id = MongoResource.insert({'test': 'test'})
        MongoResource.update_dict(object_id, {'blah': 'blah'})
        result = MongoResource.find_one(object_id)
        self.assertDictEqual(result, {'test': 'test', 'blah': 'blah', '_id': object_id})

    def test_find(self):
        for i in xrange(1, 20):
            MongoResource.insert({'test': i})

        documents = list(MongoResource.find({}))
        self.assertEqual(len(documents), 10)
        self.assertEqual(documents[0]['test'], 1)

        documents = list(MongoResource.find({}, skip=10, limit=2))
        self.assertEqual(len(documents), 2)
        self.assertEqual(documents[0]['test'], 11)

    def test_find_in(self):
        object_ids = []
        for i in xrange(1, 20):
            object_id = MongoResource.insert({'test': i})
            object_ids.append(object_id)

        documents = list(MongoResource.find_in(object_ids[:5]))
        self.assertEqual(len(documents), 5)
        for i in xrange(0, 5):
            self.assertEqual(documents[i]['test'], i + 1)

    def test_resolve(self):
        object_ids = []
        for i in xrange(1, 20):
            object_id = MongoResource.insert({'test': i})
            object_ids.append(object_id)

        documents = list(MongoResource.resolve([str(oid) for oid in object_ids[:5]]))
        self.assertEqual(len(documents), 5)
        for i in xrange(0, 5):
            self.assertEqual(documents[i]['test'], i + 1)

    def test_resolve_fail(self):
        object_ids = []
        for i in xrange(1, 20):
            object_id = MongoResource.insert({'test': i})
            object_ids.append(object_id)

        test_with = [str(oid) for oid in object_ids[:5]]
        test_with.append(str(ObjectId()))
        self.assertRaises(MissingIdsException, MongoResource.resolve, test_with)
