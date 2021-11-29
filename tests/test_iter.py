import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_basic():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()

	interface['key'] = []
	interface = interface['key']

	l = [{
		'key': i
	} for i in range(1, 4+1)]

	interface += l

	assert sorted(interface.keys()) == [0, 1, 2, 3]
	assert interface() == l


def test_valid_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.db.flushdb()
	interface.updateIndexesList()

	sessions = [{
		f'session {i}': {
			'name': i,
			'data': 'lalala'
		}
	} for i in range(5)]

	interface['sessions'] = sessions

	assert len(interface['sessions'].keys()) == 5
	assert interface['sessions']() == sessions