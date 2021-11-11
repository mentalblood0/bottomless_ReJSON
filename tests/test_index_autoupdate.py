import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_add():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface.indexes.clear()

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['sessions'].createIndex('state')
	
	interface['sessions']['f'] = {'state': 'new'}

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True

	assert interface['sessions'].filter('state', 'new') == [
        interface['sessions']['a'],
		interface['sessions']['c'],
        interface['sessions']['f']
	]