import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_simple():

	interface = RedisInterface.RedisInterface(config['db']['url'])

	assert interface['a'] + interface['b'] == interface['a']['b']


def test_complex():

	interface = RedisInterface.RedisInterface(config['db']['url'])

	assert interface['a']['b'] \
		 + interface['c']['d']['e'] \
		 + interface['f']['g'] \
		 == interface['a']['b']['c']['d']['e']['f']['g']
