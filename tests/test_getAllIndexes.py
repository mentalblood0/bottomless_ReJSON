import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_basic():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['sessions'].createIndex('state')

	print(interface.indexes())
	print(interface.getAllIndexes())

	assert interface.getAllIndexes() == [['sessions', 'state']]