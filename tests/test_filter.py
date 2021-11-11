import pytest

from tests import config
from bottomless_ReJSON import RedisInterface



def test_append():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()

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

	interface.indexes['sessions']['state']['new'] = [
		['sessions', 'a'],
		['sessions', 'c']
	]

	# using index
	assert interface['sessions'].filter('state', 'new') == [
		interface['sessions']['a'],
		interface['sessions']['c']
	]

	# not using index (no index)
	assert interface['sessions'].filter('state', 'processed') == [
		interface['sessions']['b'],
		interface['sessions']['d']
	]

	# not using index (no index)
	assert interface['sessions'].filter('state', 'erroneous') == [
		interface['sessions']['e']
	]