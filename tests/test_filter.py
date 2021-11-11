import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_append():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface.indexes.clear()

	interface['sessions'] = {
		'a': {
			'state': 'new'
		},
		'b': {
			'state': 'processed'
		},
		'c': {
			'state': 'new'
		},
		'd': {
			'state': 'processed'
		},
		'e': {
			'state': 'erroneous'
		}
	}

	interface.indexes['sessions']['__index__']['state'] = {
		'new': [
			['sessions', 'a'],
			['sessions', 'c']
		],
		'processed': [
			['sessions', 'b'],
			['sessions', 'd']
		],
		'erroneous': [
			['sessions', 'e']
		]
	}

	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['a'],
		interface['sessions']['c']
	]

	assert interface['sessions'].filter('state', 'processed') == [
		interface['sessions']['b'],
		interface['sessions']['d']
	]

	assert interface['sessions'].filter('state', 'erroneous') == [
		interface['sessions']['e']
	]