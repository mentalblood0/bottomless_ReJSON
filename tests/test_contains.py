from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_object():

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


def test_array():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface.set([
		[
			[
				'zero.zero.zero'
			],
			[
				'zero.one.zero'
			]
		]
	])

	assert 'zero.zero.zero' in interface[0][0]
	assert 'zero.one.zero' in interface[0][1]