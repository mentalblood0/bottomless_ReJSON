import pytest

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



def test_basic():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface |= {
		'1': {
			'1': {
				'1': 'one.one.one'
			},
			'2': 'one.two'
		},
		'2': 'two'
	}

	assert interface() == {
		'1': {
			'1': {
				'1': 'one.one.one'
			},
			'2': 'one.two'
		},
		'2': 'two'
	}


def test_not_empty():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	interface['key'] = {
		'a': 1
	}

	interface['key'] |= {
		'b': {
			'c': 3,
			'd': 4
		}
	}

	assert interface['key']() == {
		'a': 1,
		'b': {
			'c': 3,
			'd': 4
		}
	}


def test_not_dict():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	key = '1'

	interface[key] = False
	assert interface[key]() == False

	interface[key] = {
		'a': 1,
		'b': 2
	}
	assert interface[key]() == {
		'a': 1,
		'b': 2
	}

	interface[key] = False
	assert interface[key]() == False


def test_deep_dict_shallow_update():

	interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.clear()
	key = 'key'
	deep = {
		'a': {
			'b': 2
		},
		'c': {
			'd': 4
		},
		'f': {
			'g': 7
		}
	}
	shallow = {
		'a': {
			'e': 5
		},
		'c': 3
	}

	interface[key] = deep
	assert interface[key]() == deep

	interface[key] |= shallow
	assert interface[key]() == deep | shallow