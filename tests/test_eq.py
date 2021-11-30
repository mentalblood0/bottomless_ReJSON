from tests import config
from bottomless_ReJSON import RedisInterface



def test_valid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface['key'] = 'value'

	assert interface['key'] == 'value'


def test_invalid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	
	del interface['key']

	assert interface['key'] == None