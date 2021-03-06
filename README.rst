#########
Mongothin
#########

Mongothin is a small wrapper around pymongo. It allows you to do this::

    result = UserResource.find_one('XXXX')

Instead of::

    client = MongoClient()
    result = client['database']['users'].find_one(ObjectId('XXXX'))

Mongothin also handles retries and exponential backoff.

===========
Connections
===========

Mongothin handles clients configuration the way MongoEngine does. Actually the code is
taken from MongoEngine.

========
Resource
========

A resource wraps metadata such as client alias, collection, number of retries and proxies pymonogo collection methods
to pymongo itself.

This is how we define a resource::

    class UserResource(Resource):
        # The connection alias registered via :function:clients.register_connection.
        _alias = 'user'
        # The _id field type. Whatever is your doc_id input it will be coerced to this type.
        # Default is ObjectId. This callable must accept a single argument to whatever is used in the method
        # calls.
        _id_type = ObjectId
        # Number of retries
        _retries = 2
        # Exponential backoff base
        _delay = 0.01
        # Collection. If not specified the name of the class is used.
        _collection = 'user'
        # Shard. Make sure the shard info is in the specs
        _shard = (shard_function, shard_key,)


A Resource is heavily oriented to work with _id fields.

Data mapping
------------

Mongothin does not do data mapping. pymongo's input and output are dictionaries, so are Mongothin's. If you need input validation
you can use another library such as:

* `onctuous <https://bitbucket.org/Ludia/onctuous>`_
* `validino <https://github.com/alecthomas/validino>`_
* `FormEncode <http://www.formencode.org/en/latest/>`_
* `Schematics <https://github.com/j2labs/schematics>`_
