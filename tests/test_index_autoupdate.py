import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_add_simple():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	
	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True
	
	interface['sessions']['f'] = {'state': 'new'}

	print(interface.indexes())

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['f']
	]


def test_add_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['a'],
		interface['sessions']['c']
	]


def test_add_simple_after_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	interface['sessions']['f'] = {'state': 'new'}

	assert interface.indexes['sessions']['__index__']['state']['new']['f']() == True
	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['a'],
		interface['sessions']['c'],
		interface['sessions']['f']
	]


def test_remove_simple():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	print(interface.indexes())
	print(interface.getAllIndexes())
	print('del')
	del interface['sessions']['a']

	assert interface.indexes['sessions']['__index__']['state']['new']['a']() == None
	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['c']
	]

	# assert False


def test_remove_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	del interface['sessions']

	print('indexes', interface.indexes())

	assert interface.indexes['sessions']['__index__']['state']() == {
		state: {}
		for state in ['new', 'processed', 'erroneous']
	}
	assert interface['sessions'].filter(state='new') == []
	assert interface['sessions'].filter(state='processed') == []
	assert interface['sessions'].filter(state='erroneous') == []


def test_update_value():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'}
	}

	print('update')
	interface['sessions']['a']['state'] = 'processed'
	assert interface['sessions'].filter(state='new') == []
	assert interface['sessions'].filter(state='processed') == [
		interface['sessions']['a']
	]

	interface['sessions']['a']['state'] = 'finished'
	assert interface['sessions'].filter(state='new') == []
	assert interface['sessions'].filter(state='processed') == []
	assert interface['sessions'].filter(state='finished') == [
		interface['sessions']['a']
	]


def test_update():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

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
	print("interface['sessions'].filter(state='new')", interface['sessions'].filter(state='new'))
	assert interface['sessions'].filter(state='new') == [
		interface['sessions']['c'],
		interface['sessions']['f']
	]
	assert interface['sessions'].filter(state='processed') == [
		interface['sessions']['b']
	]
	assert interface['sessions'].filter(state='erroneous') == [
		interface['sessions']['e']
	]


def test_update_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	interface['key']['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

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

	print(f"indexes: {interface.indexes()}")

	assert interface.indexes['key']['sessions']['__index__']['state']['new']['a']() == None
	assert interface.indexes['key']['sessions']['__index__']['state']['processed']['d']() == None
	assert interface.indexes['key']['sessions']['__index__']['state']['new']['f']() == True
	assert interface['key']['sessions'].filter(state='new') == [
		interface['key']['sessions']['c'],
		interface['key']['sessions']['f']
	]
	assert interface['key']['sessions'].filter(state='processed') == [
		interface['key']['sessions']['b']
	]
	assert interface['key']['sessions'].filter(state='erroneous') == [
		interface['key']['sessions']['e']
	]


def test_clear():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	assert interface() == None

	interface['sessions'].createIndex('state')
	interface.use_indexes_cache = False
	interface.use_indexes_cache = True

	interface['sessions'] = {
		'a': {'state': 'new'},
		'b': {'state': 'processed'},
		'c': {'state': 'new'},
		'd': {'state': 'processed'},
		'e': {'state': 'erroneous'}
	}

	print('clear')
	interface.clear()

	print('interface', interface())
	assert interface() == None

	assert interface.indexes['sessions']['__index__']['state']() == {
		state: {}
		for state in ['new', 'processed', 'erroneous']
	}
	assert interface['sessions'].filter(state='new') == []
	assert interface['sessions'].filter(state='processed') == []
	assert interface['sessions'].filter(state='erroneous') == []

	# assert False