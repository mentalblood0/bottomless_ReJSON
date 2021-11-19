import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_valid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface['key'] = 'value'

	del interface['key']


def test_cascade():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	interface['1'] = 'one'
	interface['2'] = 'two'
	interface['1']['1'] = 'one.one'
	interface['1']['2'] = 'one.two'
	interface['11'] = 'oneone'

	del interface['1']

	assert interface['1']() == None
	assert interface['1'] == None
	assert interface['1']['1'] == None
	assert interface['1']['2'] == None
	assert interface['11'] == 'oneone'

	del interface['2']

	assert interface['2'] == None