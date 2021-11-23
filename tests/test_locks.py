import pytest
from time import sleep
from redis import Redis
from threading import Thread

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_update():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

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


def test_add():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	results = {'long_set': 0}

	def long_set(interface, value):
		interface.set(value)
		results['long_set'] += 1
	
	n = 1 * 10 ** 2
	value = {
		"id": "35302f1ef45e42beae445d96ab5a5fb8",
		"name": "test_session",
		"type": "any",
		"opened": 1,
		"datetime": "2021-11-23 13:38:44.641060",
		"state": "new",
		"commandCount": 0
	}
	
	setters = [
		Thread(
			target=long_set,
			args=[interface['sessions'][str(i)], value]
		)
		for i in range(n)
	]

	for s in setters:
		s.start()
		# s.join()
	
	while not results['long_set'] == n:
		sleep(0.1)
	
	assert len(interface['sessions']) == n