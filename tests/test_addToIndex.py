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

	interface.indexes['sessions']['__index__']['state'] = {}
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True
	for k in interface['sessions'].keys():
		interface['sessions'][k].addToIndex('state')
	
	assert interface.indexes['sessions']['__index__']['state'] == {
		'new': {
			'a': True,
			'c': True
		},
		'processed': {
			'b': True,
			'd': True
		},
		'erroneous': {
			'e': True
		}
	}

	assert interface['sessions'].filter(state='new') == {
		interface['sessions']['a'],
		interface['sessions']['c']
	}

	assert interface['sessions'].filter(state='processed') == {
		interface['sessions']['b'],
		interface['sessions']['d']
	}

	assert interface['sessions'].filter(state='erroneous') == {
		interface['sessions']['e']
	}