# coding=utf-8
import unittest

import minimock

import mongothin.connection
from test.unit import MockClient


class Testconnection(unittest.TestCase):
    """Test the connection module

    """

    def setUp(self):
        """Setup

        """
        super(Testconnection, self).setUp()
        mongothin.connection.register_connection('test', 'mongothin')

        self.tt = minimock.TraceTracker()
        self.mocked_database = minimock.Mock('Database', tracker=self.tt)
        self.mocked_client = MockClient(self.mocked_database, tracker=self.tt)
        minimock.mock('mongothin.connection.MongoClient', returns=self.mocked_client, tracker=self.tt)

    def tearDown(self):
        """Teardown

        """
        super(Testconnection, self).tearDown()
        mongothin.connection._connection_settings.clear()
        mongothin.connection._connections.clear()
        mongothin.connection._dbs.clear()
        minimock.restore()

    def test_register_connection(self):
        """Test register connection settings

        """
        self.assertDictEqual(mongothin.connection._connection_settings['test'], {
            'host': 'localhost',
            'is_slave': False,
            'name': 'mongothin',
            'password': None,
            'port': 27017,
            'read_preference': False,
            'slaves': [],
            'username': None
        })

    def test_get_client(self):
        """Test getting a connection

        """
        connection = mongothin.connection.get_connection('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.connection.MongoClient(",
            "   host='localhost',",
            "   port=27017,",
            "   read_preference=False)",
        ]))
        self.assertIs(connection, self.mocked_client)

    def test_get_client_unknown(self):
        """Test getting a connection

        """
        self.assertRaises(mongothin.connection.ConnectionError, mongothin.connection.get_connection, 'argh')

    def test_get_client_invalid(self):
        """Test getting a connection

        """
        minimock.restore()
        mongothin.connection.register_connection('invalid', 'mongothin', host='localhost', invalidaram="invalid")
        self.assertRaises(mongothin.connection.ConnectionError, mongothin.connection.get_connection, 'invalid')

    def test_db(self):
        """Test get a database

        """
        db = mongothin.connection.get_db('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.connection.MongoClient(",
            "   host='localhost',",
            "   port=27017,",
            "   read_preference=False)",
            "Called getitem('mongothin')"
        ]))
        self.assertIs(db, self.mocked_database)

    def test_connect(self):
        """Test the connec method

        """
        client = mongothin.connection.connect('db', 'test1')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.connection.MongoClient(",
            "   host='localhost',",
            "   port=27017,",
            "   read_preference=False)"
        ]))
        self.assertIs(client, self.mocked_client)

    def test_disconnect(self):
        """Test disconnecting

        """
        mongothin.connection.get_db('test')
        mongothin.connection.disconnect('test')
        minimock.assert_same_trace(self.tt, '\n'.join([
            "Called mongothin.connection.MongoClient(",
            "   host='localhost',",
            "   port=27017,",
            "   read_preference=False)",
            "Called getitem('mongothin')",
            "Called disconnect()"
        ]))
        self.assertDictEqual(mongothin.connection._dbs, {})
