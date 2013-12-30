# coding=utf-8
"""
Some utilities
"""
from bson import ObjectId


def raw_updater(data):
    """
    Pass through updater
    :param data:
    :return: The data as is
    """
    return data


def default_updater(data):
    """
    The default updated, takes a dict and just add $set. This will not handle embedded documents.
    :param data:
    :return:
    """
    return {'$set': data}


def base_encode(number, alphabet='ABCDEFGHIJKLMNOPQRSTUVWXYZ'):
    """
    Encode in base 26, making sure it has 2 letters. To be used with object_id_shard
    :param number: The number to encode
    :param alphabet: The base to encode in
    """
    base_encoded = ''
    l = len(alphabet)
    while number:
        number, i = divmod(number, l)
        base_encoded = alphabet[i] + base_encoded
    while len(base_encoded) != 2:
        base_encoded = 'A' + base_encoded
    return base_encoded


def object_id_shard(specs):
    """
    create a shard id from an object_id.
    The shard id will be from AA to ZZ. This allows you to preshard easily.
    You should shard on the coumpound key {'shard_key':1, '_id':1}

    Use like this in a Resource: _shard = (object_id_shard, 'shard_key',)

    This sharder gives a uniform ditribution and good insert rate as the first key insert randomly in the index (ok because the cardinality is small)
    and appends on the secondary index (high cardinality but monothonically increasing)

    :param specs: The query specsS

    """
    _id = specs.get('_id')
    if not _id:
        return None
    if not isinstance(_id, (ObjectId, str, unicode)):
        return None
    _id = str(_id)
    i = int(_id, 16)  # get the integer value
    mod = i % (26 * 26)
    shard = base_encode(mod)  # This gives me a string from AA to ZZ
    return shard
