import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_append():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	
	interface['key'] = []

	m = [i for i in range(10)]
	inner = interface['key']
	inner += m

	assert len(interface['key']) == len(m)

	assert interface['key'] == m