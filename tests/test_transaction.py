import uuid
import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_interrupt():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	db = interface.db

	key = 'default'
	value = uuid.uuid4().hex

	def transaction_function(pipe):

		if pipe.get(key) != value:
			pipe.set(key, value)
		
		pipe.multi()
		pipe.jsonset('root', '.', {'a': 1})
	
	db.transaction(transaction_function, key)
	assert db.get(key) == value

def test_delete_after_multi():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()

	db = interface.db

	key = 'default'

	db.set('del', 'lalala')

	def transaction_function(pipe):
		pipe.multi()
		pipe.delete('del')
	
	db.transaction(transaction_function, key)
	assert db.get('del') == None