import pytest

from tests import config
from bottomless_ReJSON import makeCaching



class C:

	def __init__(self, value=None):
		self._value = value

	def set(self, value):
		self._value = value
	
	def get(self):
		return self._value
	
	def value(self):
		return self._value


def test_simple():

	c = C()
	assert c.get() == None
	assert c.value() == None

	c.set(1)
	assert c.get() == 1
	assert c.value() == 1

	makeCaching(c, ['get', 'value'])

	assert c.get() == 1
	assert c.value() == 1
	
	c.set(2)
	assert c.get() == 1
	assert c.value() == 1