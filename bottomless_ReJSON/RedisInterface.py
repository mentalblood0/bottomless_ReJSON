from rejson import Client, Path
from functools import cached_property

from . import Calls
from .common import *
from .calls import *



getAllIndexes__use_cache = True
getAllIndexes__cache = None

def getAllIndexes(self):

	global getAllIndexes__cache

	if (not getAllIndexes__use_cache) or (getAllIndexes__cache == None):

		result = [self.indexes]

		while True:
			result = [
				e 
				for r in result
				for e in list(r)
				if not '__index__' in r.path
			]
			if all(['__index__' in e.path for e in result]):
				break
		
		getAllIndexes__cache = [
			e.path[:-2] + [e.path[-1]]
			for r in result
			for e in list(r)
		]
	
	return getAllIndexes__cache


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
	def host(self):
		return self.db.connection_pool.connection_kwargs['host']
	
	@property
	def port(self):
		return self.db.connection_pool.connection_kwargs['port']
	
	@property
	def use_indexes_cache(self):
		return getAllIndexes__use_cache
	
	@use_indexes_cache.setter
	def use_indexes_cache(self, value: bool):
		
		global getAllIndexes__cache
		if value == False:
			getAllIndexes__cache = None
		else:
			getAllIndexes__cache = self.getAllIndexes()
		
		global getAllIndexes__use_cache
		getAllIndexes__use_cache = value
	
	@property
	def parent(self):
		if not self.path:
			return None
		else:
			return RedisInterface(self.db, self.path[:-1], self.root_key)
	
	@cached_property
	def ReJSON_path(self):
		return composeRejsonPath(self.path)
	
	@property
	def _path(self):
		return self.ReJSON_path
	
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
	
	def getAllIndexes(self):
		return getAllIndexes(self)

	def isIndexExists(self, field):
		return (self.path + [field]) in self.getAllIndexes()
	
	def addToIndex(self, field, temp=None, value=None):
		
		if not self.parent or not self.parent.isIndexExists(field):
			return False

		if value == None:
			value = self[field]()
		
		if (value == None) or (type(value) in [dict, list]):
			return False
		
		index = self.parent.getIndex(field)
		(index[value][self.path[-1]]).set(True, temp)

		return True
	
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
		
		# print('removeFromIndexes', self.path, self.getAllIndexes())

		for i in self.getAllIndexes():

			if self.path == i[:len(self.path)]:
				index_path = i[:-1] + ['__index__', i[-1]]
				print(f"fits {i}, so SET {index_path} {{}}, interface is {self.indexes()}")
				for value in RedisInterface(self.db, index_path, root_key='indexes'):
					value.set({}, temp)
				# RedisInterface(self.db, index_path, root_key='indexes').set({}, temp)
				return

		if len(self.path) > 1:
			
			path = self.path[:-1]
			keys_to_delete = []
			
			for i in self.getAllIndexes():
			
				if path == i[:len(path)]:
			
					value = self[i[-1]]()
					# print('value', value)
					if value != None:
						index_path = i[:-1] + ['__index__', i[-1], value, self.path[-1]]
						keys_to_delete.append(index_path)
			
			if keys_to_delete:
				for path in keys_to_delete:
					RedisInterface(self.db, path, root_key='indexes').clear(temp)
				return
		
		if len(self.path) > 2:
			path = self.path[:-2] + [self.path[-1]]
			# print('path', path)
			for i in self.getAllIndexes():
				if path == i[:len(path)]:
					index_path = path[:-1] + ['__index__', path[-1]]
					index = RedisInterface(self.db, index_path, root_key='indexes')
					for value in index:
						value[self.path[-2]].clear(temp)
					# print(i, index_path)
		
		# keys = self.keys()
		# if keys:
		# 	for k in keys:
		# 		self[k].removeFromIndexes(temp)
		# else:
		# 	if self.parent:
		# 		self.parent.removeFromIndex(self.path[-1], temp)
	
	def createIndex(self, field):

		if self.isIndexExists(field):
			return
		
		temp = Calls()
		
		index = self.getIndex(field)
		index.set({}, temp)

		for e in self:
			e.addToIndex(field, temp)
		
		temp(self.db)
	
	def filter(self, field, value):

		if not self.isIndexExists(field):
			raise NotImplementedError(f"Index not exists: {self} {field}")

		return [
			self[k]
			for k in self.getIndex(field)[value].keys()
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

		temp = Calls()

		for key, value in other.items():
			self[key].set(value, temp)
		
		temp(self.db)
	
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