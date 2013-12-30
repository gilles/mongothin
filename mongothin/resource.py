# coding=utf-8

"""
The main ressource object
"""

import logging
from bson import ObjectId
from pymongo.errors import AutoReconnect
import time
from mongothin import raw_updater, default_updater
from mongothin.connection import DEFAULT_CONNECTION_NAME, get_db


class ResourceMeta(type):
    """
    Resource metaclass to get the collection name based on the class name.
    Also adds a logger
    """

    def __new__(mcs, name, bases, dct):
        if '_collection' not in dct:
            dct['_collection'] = name
        dct['log'] = logging.getLogger('%s.%s' % (dct['__module__'], name))
        return super(ResourceMeta, mcs).__new__(mcs, name, bases, dct)


class MissingIdsException(Exception):
    """ Used for :method:Resouce.resolve
    """
    pass


class Resource(object):
    """
    A Resource is a small wrapper around pymongo allowing you do write this:

        >>> Resource.find_one('12354')

    instead of:

        db['collection'].find_one(ObjectId('12354'))

    It also handles retry and exponential backoff. The difference with other ORM is that a Resource does not
    do type mapping. You have to validate your data via another library (os far I counted 7 in python to do this,
    pick your poison). Also it will return a dict, same as pymongo, unless when you declare something else.

    The default methods expects you to work with the _id field, create your own methods if you have a different access pattern.

    How to use a Resource:

        >>> class UserResource(Resource):
        >>>     _alias = 'user' # The connection alias registered via :function:clients.register_connection.
        >>>     # The _id field type. Whatever is your doc_id input it will be coerced to this type.
        >>>     # Default is ObjectId. This callable must accept a single argument to whatever is used in the method
        >>>     # calls.
        >>>     _id_type = ObjectId
        >>>     # Number of retries
        >>>     _retries = 2
        >>>     # Exponential backoff base
        >>>     _delay = 0.01
        >>>     # Collection. If not specified the name of the class is used.
        >>>     _collection = 'user'
        >>>     # Shard info. Make sure the query contains the shard info
        >>>     _shard = (sharder_function, field,)

    """

    __metaclass__ = ResourceMeta

    _alias = DEFAULT_CONNECTION_NAME

    _id_type = ObjectId

    _retries = 0
    _delay = 0.01

    _shard = None

    @classmethod
    def _make_specs(cls, doc_id=None, specs=None):
        """
        Combine id and extra specs
        :param doc_id: The id of the object
        :param specs:
        :return:
        """
        final_specs = {}
        if doc_id:
            final_specs['_id'] = cls._id_type(doc_id)
        if specs:
            final_specs.update(specs)
        cls._add_shard(final_specs)
        return final_specs

    @classmethod
    def _add_shard(cls, specs):
        """
        Make sure shard information is here
        """
        if cls._shard:
            try:
                shard = cls._shard[0](specs)
                if shard:
                    specs[cls._shard[1]] = shard
            except Exception as exc:
                cls.log.warning("Can't compute shard value for: %s -> %s" % (specs, exc))
        return specs

    @classmethod
    def _get_db(cls):
        db = get_db(cls._alias)
        return db

    @classmethod
    def _get_collection(cls):
        collection = cls._get_db()[getattr(cls, '_collection')]
        return collection

    @classmethod
    def _make_call(cls, function, *args, **kwargs):
        for n in xrange(0, cls._retries + 1):
            try:
                collection = cls._get_collection()
                function = getattr(collection, function)
                return function(*args, **kwargs)
            except AutoReconnect:
                time.sleep(cls._delay * (2 ** n))
        raise

    @classmethod
    def insert(cls, document, doc_id=None):
        """
        Insert a new document
        :param document: The document to insert
        :param doc_id: The _id field for this document.
            If None an ObjectId will be generated. It can be a callable.
        """
        if not doc_id:
            doc_id = ObjectId
        if callable(doc_id):
            doc_id = doc_id()

        document['_id'] = doc_id
        cls._add_shard(document)

        cls._make_call('insert', document)
        return doc_id


    @classmethod
    def update(cls, doc_id, document, specs=None, updater=raw_updater, *args, **kwargs):
        """ Update an existing document

        :param doc_id: The document _id to modify
        :param document: The update document.
            This can a full fledge update document in which case you should use the :function:raw_updater
            or a dictionnary in which case you can use the :function:default_updater to add the $set directive
        :param specs: Extra specs to select the document. Will be combined with doc_id
        :param updater: A callable used to mutate the update document before making the call
        :param args: Extra positional parameters for the call to :method:pymongo.collections.update
        :param kwargs: Extra keyword parameters for the call to :method:pymongo.collections.update
        :rtype : int
        """
        document = updater(document)
        ret = cls._make_call('update', cls._make_specs(doc_id, specs), document, *args, **kwargs)
        if ret:
            return ret['n']

    @classmethod
    def update_dict(cls, doc_id, document, specs=None, *args, **kwargs):
        """ Update an existing document using the default_updater

        :param doc_id: The document _id to modify
        :param document: The update document. A dictionnary to which $set will be added®
        :param specs: Extra specs to select the document. Will be combined with doc_id
        :param args: Extra positional parameters for the call to :method:pymongo.collections.update
        :param kwargs: Extra keyword parameters for the call to :method:pymongo.collections.update
        :rtype : int
        """
        return cls.update(doc_id, document, specs, default_updater, *args, **kwargs)

    @classmethod
    def remove(cls, doc_id, specs=None):
        """ Remove an existing document
        :param doc_id: The id of the document to remove. This can be None and use specs only
        :param specs: Extra specs to locate the document to remove
        """
        ret = cls._make_call('remove', cls._make_specs(doc_id, specs))
        if ret:
            return ret['n']

    @classmethod
    def find_one(cls, doc_id, specs=None, *args, **kwargs):
        """ Find one document
        :param doc_id: The id of the document to find. This can be None and use specs only
        :param specs: Extra specs to locate the document
        :param args: Passed to the driver as is
        :param kwargs: Passed to the driver as is
        """
        return cls._make_call('find_one', cls._make_specs(doc_id, specs), *args, **kwargs)

    @classmethod
    def find(cls, specs, skip=0, limit=10, *args, **kwargs):
        """ Find several documents
        :param specs: Extra specs to locate the document
        :param skip: Documents to skip. Since you want to paginate when querying multiple documents it's here and not in kwargs
        :param limit: Documents to return. Since you want to paginate when querying multiple documents it's here and not in kwargs
        :param args: Passed to the driver as is
        :param kwargs: Passed to the driver as is
        """
        return cls._make_call('find', specs, skip=skip, limit=limit, *args, **kwargs)

    @classmethod
    def find_in(cls, doc_ids, *args, **kwargs):
        """ Find documents in a list if ids
        :param doc_ids: A list of ids to find
        :param args: Passed to the driver as is
        :param kwargs: Passed to the driver as is
        """
        specs = {'_id': {'$in': [cls._id_type(_id) for _id in doc_ids]}}
        return cls._make_call('find', specs, *args, **kwargs)

    @classmethod
    def resolve(cls, doc_ids, *args, **kwargs):
        """ Find documents in a list of ids. Raise an exception if an id is missing
        :param doc_ids: A list of ids to find
        :param args: Passed to the driver as is
        :param kwargs: Passed to the driver as is
        """
        from_mongo = []
        if doc_ids:
            doc = cls.find_in(doc_ids, *args, **kwargs)
            from_mongo = list(doc)
            if len(from_mongo) != len(doc_ids):
                ids_from_mongo = [mongo['_id'] for mongo in from_mongo]
                missing_ids = set(doc_ids) - set(ids_from_mongo)
                if missing_ids:
                    raise MissingIdsException(missing_ids)
        return from_mongo
