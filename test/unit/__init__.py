# coding=utf-8
__author__ = 'gillesdevaux'


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
