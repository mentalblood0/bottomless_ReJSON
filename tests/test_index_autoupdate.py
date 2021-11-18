import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_add_simple():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()
	
	interface['sessions'].createIndex('state')
	
	interface['sessions']['f'] = {'state': 'new'}

	print(interface.indexes())

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['f']
	]


def test_add_complex():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['sessions'].createIndex('state')

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['a'],
		interface['sessions']['c']
	]


def test_add_simple_after_complex():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['sessions'].createIndex('state')

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['sessions']['f'] = {'state': 'new'}

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['a'],
		interface['sessions']['c'],
		interface['sessions']['f']
	]


def test_remove_simple():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['sessions'].createIndex('state')

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	del interface['sessions']['a']

	assert interface.indexes['sessions']['__index__']['state']['new']['a']() == None
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['c']
	]


def test_remove_complex():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['sessions'].createIndex('state')

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	del interface['sessions']

	print('indexes', interface.indexes())

	for state_value in ['new', 'processed', 'erroneous']:
		assert interface.indexes['sessions']['__index__']['state'][state_value]() == {}
	assert interface['sessions'].filter('state', 'new') == []
	assert interface['sessions'].filter('state', 'processed') == []
	assert interface['sessions'].filter('state', 'erroneous') == []


def test_update():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['sessions'].createIndex('state')

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['sessions'] = {
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'e': {'state': 'erroneous'},
		'f': {'state': 'new'}
	}

	assert interface.indexes['sessions']['__index__']['state']['new']['a']() == None
	assert interface.indexes['sessions']['__index__']['state']['processed']['d']() == None
	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	print("interface['sessions'].filter('state', 'new')", interface['sessions'].filter('state', 'new'))
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['c'],
		interface['sessions']['f']
	]
	assert interface['sessions'].filter('state', 'processed') == [
		interface['sessions']['b']
	]
	assert interface['sessions'].filter('state', 'erroneous') == [
		interface['sessions']['e']
	]


def test_update_complex():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['key']['sessions'].createIndex('state')

	interface['key']['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['key'] = {
		'sessions': {
			'b': {'state': 'processed'},
			'c': {'state': 'new'},
			'e': {'state': 'erroneous'},
			'f': {'state': 'new'}
		}
	}

	assert interface.indexes['key']['sessions']['__index__']['state']['new']['a']() == None
	assert interface.indexes['key']['sessions']['__index__']['state']['processed']['d']() == None
	assert interface.indexes['key']['sessions']['__index__']['state']['new']['f']() == True
	assert interface['key']['sessions'].filter('state', 'new') == [
		interface['key']['sessions']['c'],
		interface['key']['sessions']['f']
	]
	assert interface['key']['sessions'].filter('state', 'processed') == [
		interface['key']['sessions']['b']
	]
	assert interface['key']['sessions'].filter('state', 'erroneous') == [
		interface['key']['sessions']['e']
	]