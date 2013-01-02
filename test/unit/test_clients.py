# coding=utf-8
import unittest
import minimock
from pymongo import MongoClient
from pymongo.son_manipulator import SONManipulator

import mongothin.clients


class MockClient(object):
    """Mock objects are unsubscriptable"""

    def __init__(self, mocked_database, tracker):
        super(MockClient, self).__init__()
        self.tracker = tracker
        self.mocked_database = mocked_database

    def __getitem__(self, item):
        self.tracker.call('getitem', item)
        return self.mocked_database

    def disconnect(self):
        self.tracker.call('disconnect')


class Manipulator(SONManipulator):
    def transform_incoming(self, son, collection):
        return super(Manipulator, self).transform_incoming(son, collection)


class TestClients(unittest.TestCase):
    """Test the clients module

    """

    def setUp(self):
        """Setup

        """
        super(TestClients, self).setUp()
        mongothin.clients.register_connection('test', 'mongothin', host='localhost')

        self.tt = minimock.TraceTracker()
        self.mocked_database = minimock.Mock('Database', tracker=self.tt)
        self.mocked_client = MockClient(self.mocked_database, tracker=self.tt)
        minimock.mock('mongothin.clients.pymongo.MongoClient', returns=self.mocked_client, tracker=self.tt)

    def tearDown(self):
        """Teardown

        """
        super(TestClients, self).tearDown()
        mongothin.clients._clients_settings.clear()
        mongothin.clients._clients.clear()
        mongothin.clients._dbs.clear()
        minimock.restore()

    def test_register_clients(self):
        """Test register clients settings

        """
        self.assertDictEqual(mongothin.clients._clients_settings['test'], {
            'client': 'MongoClient',
            'database': 'mongothin',
            'host': 'localhost'
        })

    def test_get_client(self):
        """Test getting a clients

        """
        clients = mongothin.clients.get_client('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.clients.pymongo.MongoClient(host='localhost')"
        ]))
        self.assertIs(clients, self.mocked_client)

    def test_get_client_unknown(self):
        """Test getting a clients

        """
        self.assertRaises(mongothin.clients.ClientError, mongothin.clients.get_client, 'argh')

    def test_get_client_invalid(self):
        """Test getting a clients

        """
        minimock.restore()
        mongothin.clients.register_connection('invalid', 'mongothin', host='localhost', invalidaram="invalid")
        self.assertRaises(mongothin.clients.ClientError, mongothin.clients.get_client, 'invalid')

    def test_db(self):
        """Test get a database

        """
        db = mongothin.clients.get_db('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.clients.pymongo.MongoClient(host='localhost')",
            "Called getitem('mongothin')"
        ]))
        self.assertIs(db, self.mocked_database)

    def test_connect(self):
        """Test the connec method

        """
        client = mongothin.clients.connect('db', 'test1')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.clients.pymongo.MongoClient()"
        ]))
        self.assertIs(client, self.mocked_client)

    def test_disconnect(self):
        """Test disconnecting

        """
        mongothin.clients.get_db('test')
        mongothin.clients.disconnect('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.clients.pymongo.MongoClient(host='localhost')",
            "Called getitem('mongothin')",
            "Called disconnect()"
        ]))
        self.assertDictEqual(mongothin.clients._clients, {})
        self.assertDictEqual(mongothin.clients._dbs, {})

    def test_manipulators(self):
        config = {
            'host': 'localhost',
            'manipulators': {
                'test.unit.test_clients.Manipulator': {}
            }
        }
        mongothin.clients.register_connection('manipulator', 'mongothin', **config)
        mongothin.clients.get_db('manipulator')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.clients.pymongo.MongoClient(host='localhost')",
            "Called getitem('mongothin')",
            "Called Database.add_son_manipulator(",
            "    <test.unit.test_clients.Manipulator object at ...>)"
        ]))

