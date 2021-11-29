from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_nonexistent_key():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	assert interface['key'] == None
	assert interface['key']() == None


def test_deep():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface['1'] = 'one'
	interface['2'] = 'two'
	interface['1']['1']['1'] = 'one.one.one'
	interface['1']['2'] = 'one.two'

	assert interface() == {
		'1': {
			'1': {
				'1': 'one.one.one'
			},
			'2': 'one.two'
		},
		'2': 'two'
	}


def test_change_depth():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface['key'] = {
		'a': 1
	}
	interface['key']['a'] = {
		'b': 2
	}

	interface['key']['a'] == None
	assert interface['key']() == {
		'a': {
			'b': 2
		}
	}