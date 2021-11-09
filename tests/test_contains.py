import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_basic():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()

	interface |= {
		'sessions': {
			1: {
				'name': 'one'
			},
			'2': {
				'name': 'two'
			}
		}
	}

	assert 'name' in interface['sessions']['1']

	assert 1 in interface['sessions']
	assert '1' in interface['sessions']
	
	assert 2 in interface['sessions']
	assert '2' in interface['sessions']