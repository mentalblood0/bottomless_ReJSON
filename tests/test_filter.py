import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_with_index():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()

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
	interface.updateIndexesList()

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


def test_without_index():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()
	interface.updateIndexesList()

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	assert interface.indexes == None

	interface.updateIndexesList()
	for state_value in ['new', 'processed', 'erroneous']:
		with pytest.raises(NotImplementedError):
			interface['sessions'].filter(state=state_value)


def test_change_value():

	items_number = 10

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()
	interface['sessions'].createIndex('state')
	interface.updateIndexesList()

	interface['sessions'] = {
		str(i): {'state': 'finished'}
		for i in range(items_number)
	}
	interface['sessions'][str(items_number // 2)]['state'] = 'new'


def test_n_index():

	items_number = 10
	index_number = 10

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()

	for i in range(index_number):
		interface['sessions'].createIndex(f"property_{i}")
	interface.updateIndexesList()

	interface['sessions'] = {
		str(j): {
			f"property_{i}": bool(j)
			for i in range(index_number)
		}
		for j in range(items_number)
	}
	interface['sessions'][str(items_number // 2)]['state'] = 'new'

	result = interface['sessions'].filter(**{
		f"property_{i}": False
		for i in range(index_number)
	})

	assert result == {interface['sessions']['0']}