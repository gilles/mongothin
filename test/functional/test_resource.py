# coding=utf-8
import unittest
from bson import ObjectId
from pymongo.son_manipulator import SONManipulator
import mongothin.clients
from mongothin.resource import Resource, MissingIdsException


class MongoResource(Resource):
    _collection = 'argh'


class Manipulator(SONManipulator):
    def __init__(self, param=None):
        super(Manipulator, self).__init__()
        self.param = param

    def transform_incoming(self, son, collection):
        son['manipulated'] = self.param
        return son


class ManipulatedResource(Resource):
    _alias = 'manipulator'
    _collection = 'manipulated'


class DocumentClass(dict):
    def __init__(self, *args, **kwargs):
        super(DocumentClass, self).__init__(*args, **kwargs)

    def to_api(self):
        self['toapi'] = True
        return self


class DocumentResource(Resource):
    _alias = 'documentclass'
    _collection = 'documentclass'


class AsClassResource(Resource):
    _collection = 'asclass'
    _as_class = DocumentClass


class TestResource(unittest.TestCase):
    def setUp(self):
        """Setup

        """
        super(TestResource, self).setUp()
        mongothin.clients.register_connection('default', 'mongothin')
        self.client = mongothin.clients.get_client('default')

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

    def test_manipulators(self):
        config = {
            'host': 'localhost',
            'manipulators': {
                'test.functional.test_resource.Manipulator': {
                    'param': 'argh'
                }
            }
        }
        mongothin.clients.register_connection('manipulator', 'mongothin', **config)
        object_id = ObjectId()
        ManipulatedResource.insert({'test': 'test'}, object_id)
        result = ManipulatedResource.find_one(object_id)
        self.assertDictEqual(result, {
            '_id': object_id,
            'test': 'test',
            'manipulated': 'argh'
        })

    def test_document_class(self):
        config = {
            'host': 'localhost',
            'document_class': 'test.functional.test_resource.DocumentClass'
        }
        mongothin.clients.register_connection('documentclass', 'mongothin', **config)
        object_id = ObjectId()
        DocumentResource.insert({'test': 'test'}, object_id)
        result = DocumentResource.find_one(object_id)
        self.assertTrue(result.to_api()['toapi'])

    def test_as_class(self):
        object_id = ObjectId()
        AsClassResource.insert({'test': 'test'}, object_id)
        result = AsClassResource.find_one(object_id)
        self.assertTrue(result.to_api()['toapi'])
