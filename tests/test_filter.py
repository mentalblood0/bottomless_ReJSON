import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_with_index():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface.indexes['sessions']['__index__']['state'] = {
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
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['a'],
		interface['sessions']['c']
	]

	assert interface['sessions'].filter(state='processed') == [
		interface['sessions']['b'],
		interface['sessions']['d']
	]

	assert interface['sessions'].filter(state='erroneous') == [
		interface['sessions']['e']
	]


def test_without_index():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	assert interface.indexes == None

	interface.use_indexes_cache = False
	interface.use_indexes_cache = True
	for state_value in ['new', 'processed', 'erroneous']:
		with pytest.raises(NotImplementedError):
			interface['sessions'].filter(state=state_value)