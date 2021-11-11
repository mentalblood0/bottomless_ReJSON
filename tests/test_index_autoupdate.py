import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_add_simple():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()
	
	interface['sessions'].createIndex('state')
	
	interface['sessions']['f'] = {'state': 'new'}

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['f']
	]


def test_add_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
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

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
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

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
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

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
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

	assert interface.indexes['sessions']['__index__']['state']() == {}
	assert interface['sessions'].filter('state', 'new') == []
	assert interface['sessions'].filter('state', 'processed') == []
	assert interface['sessions'].filter('state', 'erroneous') == []