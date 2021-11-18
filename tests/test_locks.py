import pytest
from time import sleep
from redis import Redis
from threading import Thread

from tests import config
from bottomless_ReJSON import RedisInterface



def test_basic():

	interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()

	results = {'long_get': None}

	def long_set(interface, key, value):
		interface[key] = value
	
	def long_get(interface, key, valid_key, results):
		sleep(1)
		print(interface(), interface[key](), valid_key)
		results['long_get'] = (interface[key]() == valid_key)
	
	n = 1 * 10 ** 2
	key = 'key'
	value = {
		str(i+1): i+1
		for i in range(n)
	}
	
	setter = Thread(
		target=long_set,
		args=[interface, key, value]
	)

	getter = Thread(
		target=long_get,
		args=[interface[key], str(n), n, results]
	)

	setter.start()
	getter.start()

	setter.join()
	getter.join()

	assert results['long_get'] == True