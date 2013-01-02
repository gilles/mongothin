# coding=utf-8
"""
Manage connections, this is a simplified version of the code found in MongoEngine
"""

__author__ = 'gillesdevaux'

import pymongo

__all__ = ['ClientError', 'connect', 'register_connection',
           'DEFAULT_CONNECTION_NAME']

DEFAULT_CONNECTION_NAME = 'default'

from importlib import import_module


def import_class(name):
    parts = name.split('.')
    module = '.'.join(parts[0:-1])
    classname = parts[-1]
    module = import_module(module)
    return getattr(module, classname)


class ClientError(Exception):
    pass


_clients_settings = {}
_clients = {}
_dbs = {}


def register_connection(alias, database, client='MongoClient', **kwargs):
    """Register connection settings.

    :param alias: the name that will be used to refer to this connection
    :param database: the name of the database to use
    :param client: The client class to use
    :param kwargs: Parameters to initialize the client class

    """
    global _clients_settings

    conn_settings = {
        'client': client,
        'database': database,
    }
    conn_settings.update(kwargs)
    _clients_settings[alias] = conn_settings


def disconnect(alias=DEFAULT_CONNECTION_NAME):
    """Disconnect a connection. This also clears the _client and _dbs cache

    :param alias: The name of the connection to disconnect
    """
    global _clients
    global _dbs

    if alias in _clients:
        get_client(alias=alias).disconnect()
        del _clients[alias]
    if alias in _dbs:
        del _dbs[alias]


def get_client(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    """Get a connection object

    :param alias: The name of the connection
    :param reconnect: Reconnect to mongodb.
    :rtype : Mongo client class such as :class:MongoClient or :class:MongoReplicasetClient
    """
    global _clients
    if reconnect:
        disconnect(alias)

    if alias not in _clients:
        if alias not in _clients_settings:
            msg = 'Connection with alias "%s" has not been defined' % alias
            raise ClientError(msg)
        conn_settings = _clients_settings[alias].copy()

        for key in ['database', 'manipulators']:
            if key in conn_settings:
                del conn_settings[key]

        connection_class = conn_settings.pop('client')
        document_class = conn_settings.pop('document_class', None)
        if document_class:
            document_class = import_class(document_class)
            conn_settings['document_class'] = document_class

        try:
            connection_class = getattr(pymongo, connection_class)
            _clients[alias] = connection_class(**conn_settings)
        except Exception as e:
            raise ClientError("Cannot connect to database %s :\n%s" % (alias, e))
    return _clients[alias]


def get_db(alias=DEFAULT_CONNECTION_NAME, reconnect=False):
    """Get a connection to a database

    :param alias: The name of the connection
    :param reconnect: Reconnect to mongodb.
    :rtype : :class:pymongo.Database
    """
    global _dbs
    if reconnect:
        disconnect(alias)

    if alias not in _dbs:
        conn = get_client(alias)
        conn_settings = _clients_settings[alias]

        manipulators = conn_settings.pop('manipulators', {})

        db = conn[conn_settings['database']]

        for manipulator, config in manipulators.iteritems():
            manipulator_class = import_class(manipulator)
            manipulator = manipulator_class(**config)
            db.add_son_manipulator(manipulator)

        _dbs[alias] = db

    return _dbs[alias]


def connect(db, alias=DEFAULT_CONNECTION_NAME, client='MongoClient', **kwargs):
    """Shortcut for :method:register_connection and :method:get_connection

    :param db: the name of the database to use
    :param alias: the name that will be used to refer to this connection
    :param client: The client class to use
    :param kwargs: Parameters to initialize the client class
    :rtype : Mongo client class such as :class:MongoClient or :class:MongoReplicasetClient
    """
    global _clients
    if alias not in _clients:
        register_connection(alias, db, client, **kwargs)

    return get_client(alias)
