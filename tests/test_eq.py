import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_valid_key():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()
	interface.clear()
	interface['key'] = 'value'

	assert interface['key'] == 'value'


def test_invalid_key():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()
	interface.clear()
	del interface['key']

	assert interface['key'] == None