import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_valid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.clear()
	interface['key'] = 'value'

	assert interface['key'] == 'value'


def test_invalid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.clear()
	del interface['key']

	assert interface['key'] == None