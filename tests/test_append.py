import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_append():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()

	interface['key'] = []
	print('1', interface())

	m = [i for i in range(10)]
	inner = interface['key']
	print('2')
	inner += m
	print('3')

	assert len(interface['key']) == len(m)

	assert interface['key'] == m