from tests import config
from bottomless_ReJSON import RedisInterface



def test_dict():

	db = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	
	db.clear()
	d = {
		'1': {
			'1': {
				'1': 'one.one.one'
			},
			'2': 'one.two'
		},
		'2': 'two'
	}
	db |= d
	assert db() == d

	db['2'] = d
	assert db['2']() == d

	db['1']['1'] = 'lalala'
	assert db['1']['1'] == 'lalala'
	assert db['1']['1']['1'] == None


def test_list():

	db = RedisInterface(host=config['db']['host'], port=config['db']['port'])

	db.clear()
	db['key'] = []
	db = db['key']

	l = [1, 2, 3]
	db += [1, 2, 3]

	i = 0
	for e in db:
		# e is RedisInterface instance, 
		# so to get data you need to call it:
		assert e() == l[i]
		assert e() == db[i]
		i += 1

	assert list(db) == [1, 2, 3]