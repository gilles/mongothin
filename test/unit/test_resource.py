# coding=utf-8
import unittest
from bson import ObjectId
import minimock
import mongothin
import mongothin.connection
from mongothin.resource import Resource


class MongoResource(Resource):
    """
    Test resource
    """
    _collection = 'argh'
    _shard = (mongothin.object_id_shard, 'shard',)


class TestResource(unittest.TestCase):
    def setUp(self):
        """Setup

        """
        super(TestResource, self).setUp()
        mongothin.connection.register_connection('default', 'mongothin')

        self.tt = minimock.TraceTracker()
        self.mocked_collection = minimock.Mock('Collection', tracker=self.tt)
        minimock.mock('mongothin.resource.Resource._get_db', returns={'argh': self.mocked_collection})

    def tearDown(self):
        """Teardown

        """
        super(TestResource, self).tearDown()
        mongothin.connection._connection_settings.clear()
        mongothin.connection._connections.clear()
        mongothin.connection._dbs.clear()
        minimock.restore()

    def test_shard_insert(self):
        MongoResource.insert({'test': 'test'})
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called Collection.insert({'test': 'test', '_id': ObjectId('...'), 'shard': '...'})"
        ]))

    def test_shard_find_one(self):
        object_id = ObjectId()
        MongoResource.find_one(object_id)
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called Collection.find_one(",
            "    {'_id': ObjectId('...'), 'shard': '...'})"
        ]))

