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
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True
	
	interface['sessions']['a'].removeFromIndex('state')
	del interface['sessions']['a']

	assert interface.indexes['sessions']['__index__']['state']['new']['a']() == None

	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['c']
	]