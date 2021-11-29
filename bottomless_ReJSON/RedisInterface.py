from rejson import Client
from functools import cached_property

from . import Calls
from .calls import *
from .common import *



class RedisInterface:

	indexes = {}

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
	def db_id(self):
		return self.db.connection_pool.connection_kwargs['db']
	
	@cached_property
	def parent(self):
		return None if not self.path else RedisInterface(self.db, self.path[:-1], self.root_key)
	
	@cached_property
	def _path(self):
		return composeRejsonPath(self.path)
	
	def keys(self):

		t = self.type
		
		if t == 'object':
			return set(self.db.jsonobjkeys(self.root_key, self._path))
		
		elif t == 'array':
			return {i for i in range(self.db.jsonarrlen(self.root_key, self._path))}
		
		else:
			return set()
	
	@property
	def type(self):
		return self.db.jsontype(self.root_key, self._path)
	
	def __getitem__(self, key):
		return RedisInterface(self.db, self.path + [key], root_key=self.root_key)

	def getIndex(self, field):
		return (self.indexes + self)['__index__'][field]
	
	@cached_property
	def _indexes_list_key(self):
		return f"{self.host}:{self.port}:{self.db_id}"
	
	def updateIndexesList(self):

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
		
		self.indexes_list = [
			e.path[:-2] + [e.path[-1]]
			for r in result
			for e in list(r)
		]
	
	@property
	def indexes_list(self):
		return self.__class__.indexes[self._indexes_list_key]
	
	@indexes_list.setter
	def indexes_list(self, value):
		self.__class__.indexes[self._indexes_list_key] = value

	def isIndexExists(self, field):
		return (self.path + [field]) in self.indexes_list
	
	def addToIndex(self, field, temp=None, value=None):
		
		if not self.parent or not self.parent.isIndexExists(field):
			return False

		if value == None:
			value = self[field]()
		
		if (value == None) or (type(value) in [dict, list]):
			return False
		
		index = self.parent.getIndex(field)
		index[value][self.path[-1]].set(True, temp)

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

		for i in self.indexes_list:

			if self.path == i[:len(self.path)]:
				index_path = i[:-1] + ['__index__', i[-1]]
				for value in RedisInterface(self.db, index_path, root_key='indexes'):
					value.set({}, temp)
				return

		if len(self.path) > 1:
			
			path = self.path[:-1]
			keys_to_delete = []
			
			for i in self.indexes_list:
			
				if path == i[:len(path)]:
			
					value = self[i[-1]]()
					if value != None:
						index_path = i[:-1] + ['__index__', i[-1], value, self.path[-1]]
						keys_to_delete.append(index_path)
			
			if keys_to_delete:
				for path in keys_to_delete:
					RedisInterface(self.db, path, root_key='indexes').clear(temp)
				return
		
		if len(self.path) > 2:
			path = self.path[:-2] + [self.path[-1]]
			for i in self.indexes_list:
				if path == i[:len(path)]:
					index_path = path[:-1] + ['__index__', path[-1]]
					index = RedisInterface(self.db, index_path, root_key='indexes')
					for value in index:
						value[self.path[-2]].clear(temp)
	
	def createIndex(self, field):

		if self.isIndexExists(field):
			return
		
		new_index_signature = self.path + [field]
		if not new_index_signature in self.indexes_list:
			self.indexes_list.append(new_index_signature)

		temp = Calls()
		
		self.getIndex(field).set({}, temp)
		for e in self:
			e.addToIndex(field, temp)
		
		temp(self.db)
	
	def filter(self, **kwargs):

		for field in kwargs.keys():
			if not self.isIndexExists(field):
				raise NotImplementedError(f"Index not exists: {self} {field}")
		
		keys = set()
		for field, value in kwargs.items():
			if not keys:
				keys = self.getIndex(field)[value].keys()
				if not keys:
					return set()
			else:
				if len(keys) < len(self.getIndex(field)[value]):
					keys = {
						k
						for k in keys
						if self.getIndex(field)[value][k].type != None
					}
				else:
					keys &= self.getIndex(field)[value].keys()
		
		return {
			self[k]
			for k in keys
		}

	def set(self, value, temp=None):

		new_call = SetCall(('jsonset', (self.root_key, self.path, value)))
		
		if temp != None:
			temp.append(new_call)
		else:
			Calls([new_call])(self.db)

	def __setitem__(self, key, value):
		if not isinstance(value, RedisInterface):
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
			return self[str(key)].type != None
		
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
		for k in self.keys():
			yield self[k]

	def __repr__(self):
		return f'<{self.__class__.__name__} root_key="{self.root_key}" path="{self._path}">'
	
	def __bool__(self):
		return True
	
	def __hash__(self):
		return hash((
			str(e)
			for e in [
				self.host,
				self.port,
				self.db_id,
				self.root_key,
				self.path
			]
		))