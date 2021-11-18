import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_string():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['key'] = '1'
	assert interface['key']() == '1'


def test_int():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['key'] = 1
	assert type(interface['key']()) == int
	assert interface['key'] == 1


def test_float():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['key'] = 1.0
	assert type(interface['key']()) == int
	assert interface['key'] == 1.0

	interface['key'] = 1.1
	assert type(interface['key']()) == float
	assert interface['key'] == 1.1


def test_bool():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	interface['key'] = True
	assert type(interface['key']()) == bool
	assert interface['key'] == True

	interface['key'] = False
	assert type(interface['key']()) == bool
	assert interface['key'] == False


def test_None():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	assert type(interface()) == type(None)
	assert interface == None