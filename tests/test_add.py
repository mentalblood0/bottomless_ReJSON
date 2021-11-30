from tests import config
from bottomless_ReJSON import RedisInterface



def test_simple():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()

	assert interface['a'] + interface['b'] == interface['a']['b']


def test_complex():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()

	assert interface['a']['b'] \
		 + interface['c']['d']['e'] \
		 + interface['f']['g'] \
		 == interface['a']['b']['c']['d']['e']['f']['g']
