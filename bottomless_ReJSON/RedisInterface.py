import json
from rejson import Client, Path
from functools import cached_property

from . import Calls
from .common import *
from .calls import *


class RedisInterface:

	def __init__(
		self, 
		db=None,
		path=[],
		root_key='root',
		host=None,
		port=None,
		indexes=None
	):
		
		self.__db = db if isinstance(db, Client) else Client(host=host, port=port, decode_responses=True)
		self.__path = path
		self.__root_key = root_key

		if self.root_key != 'indexes':
			self.indexes = indexes or RedisInterface(self.db, root_key='indexes')
	
	@property
	def db(self):
		return self.__db
	
	@property
	def path(self):
		return self.__path
	
	@property
	def root_key(self):
		return self.__root_key
	
	@property
	def parent(self):
		if not self.path:
			return None
		else:
			return RedisInterface(self.db, self.path[:-1], root_key=self.root_key)
	
	@cached_property
	def ReJSON_path(self):
		return composeRejsonPath(self.path)
	
	@property
	def _path(self):
		return Path(self.ReJSON_path)
	
	def keys(self):

		t = self.type
		
		if t == 'object':
			return self.db.jsonobjkeys(self.root_key, self._path)
		
		elif t == 'array':
			return [i for i in range(self.db.jsonarrlen(self.root_key, self._path))]
		
		else:
			return []
	
	@property
	def type(self):
		return self.db.jsontype(self.root_key, self._path)
	
	def __getitem__(self, key):
		return RedisInterface(self.db, self.path + [key], root_key=self.root_key)
	
	def getIndex(self, field):
		return (self.indexes + self)['__index__'][field]

	def isIndexExists(self, field):
		return self.getIndex(field).type == 'object'
	
	def addToIndex(self, field, temp=None, value=None):

		if not self.parent:
			return False
		
		if not self.parent or not self.parent.isIndexExists(field):
			return False

		if value == None:

			value = self[field]()
			if not value:
				return False
		
		index = self.parent.getIndex(field)
		(index[value][self.path[-1]]).set(True, temp)

		return True
	
	def removeFromIndex(self, field, temp=None):

		if not self.parent or not self.parent.isIndexExists(field):
			return False
		
		index = self.parent.getIndex(field)
		
		value = self[field]()
		if not value:
			return
		
		index[value].__delitem__(self.path[-1], temp)
		if not len(index[value]):
			index.__delitem__(value, temp)
	
	def addToIndexes(self, payload, temp=None):

		if not hasattr(self, 'indexes'):
			return

		if type(payload) == dict:
			for k in payload.keys():
				self[k].addToIndexes(payload[k], temp)
		else:
			if self.parent:
				self.parent.addToIndex(self.path[-1], temp, value=payload)
	
	def removeFromIndexes(self, temp=None):
		
		if not hasattr(self, 'indexes'):
			return
		
		if self.type == 'object':
			for k in self.keys():
				self[k].removeFromIndexes(temp)
		else:
			if self.parent:
				self.parent.removeFromIndex(self.path[-1], temp)
	
	def createIndex(self, field):

		if self.isIndexExists(field):
			return
		
		index = self.getIndex(field)
		index.set({})

		temp = Calls()
		for e in self:
			e.addToIndex(field, temp)
		
		temp(self.db)
	
	def filter(self, field, value):

		if not self.isIndexExists(field):
			return [
				self[k]
				for k in self.keys()
				if self[k][field] == value
			]

		index = self.getIndex(field)

		keys = index[value]() or {}

		return [
			self[k]
			for k in keys
		]

	def set(self, value, temp=None):

		new_call = SetCall(('jsonset', (self.root_key, self.path, value)))
		
		if temp != None:
			temp.append(new_call)
		else:
			Calls([new_call])(self.db)

	def __setitem__(self, key, value):

		if isinstance(value, RedisInterface):
			return

		self[key].set(value)
	
	def clear(self, temp=None):

		new_call = DeleteCall(('jsondel', (self.root_key, self.path)))
		
		if temp != None:
			temp.append(new_call)
		else:
			Calls([new_call])(self.db)

	def __delitem__(self, key, temp=None):
		self[key].clear(temp)
	
	def __eq__(self, other):

		if isinstance(other, RedisInterface):
			return ((self.root_key == other.root_key) and (self.path == other.path)) or (self() == other())
		else:
			return self() == other
	
	def __call__(self):

		try:
			result = self.db.jsonget(self.root_key, self._path)
		except Exception:
			result = None
		
		return result

	def __contains__(self, key):

		t = self.type
		
		if t == 'object':
			return str(key) in self.db.jsonobjkeys(self.root_key, self._path)
		
		elif t == 'array':
			return self.db.jsonarrindex(self.root_key, self._path, key) != -1
	
	def update(self, other: dict):
		for key, value in other.items():
			self[key].set(value)
	
	def __ior__(self, other: dict): # |=
		self.update(other)
		return self
	
	def __len__(self):

		d = {
			'object': 'jsonobjlen',
			'array': 'jsonarrlen'
		}

		t = self.type
		if not t in d:
			return 0
		else:
			return getattr(self.db, d[t])(self.root_key, self._path)
	
	def append(self, value):
		self.db.jsonarrappend(self.root_key, self._path, value)
	
	def __iadd__(self, other: list): # +=
		
		self.db.jsonarrappend(self.root_key, self._path, *other)
		
		return self
	
	def __add__(self, other):

		if isinstance(other, RedisInterface):
			other_path = other.path
		elif type(other) == list:
			other_path = other

		return RedisInterface(self.db, self.path + other_path, root_key=self.root_key)
	
	def __iter__(self):
		for k in sorted(self.keys()):
			yield self[k]

	def __repr__(self):
		return f"<{self.__class__.__name__} root_key=\"{self.root_key}\" path=\"{self.ReJSON_path}\">"
	
	def __bool__(self):
		return True