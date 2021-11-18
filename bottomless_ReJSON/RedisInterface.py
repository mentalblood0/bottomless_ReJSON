import json
from types import new_class
from redis import Redis
from rejson import Client, Path
from redis.client import Pipeline
from functools import cached_property



def joinDicts(*args):

	result = {}

	for d in args:
		
		for key in d:
			if key in result:
				result[key] = joinDicts(result[key], d[key])
			else:
				result[key] = d[key]
	
	return result


def aggregateSetCalls(calls):

	joined = {}

	for root_key, path, value in calls:
		
		key = json.dumps(path)
		
		if not root_key in joined:
			joined[root_key] = {}
		
		if not key in joined[root_key]:
			joined[root_key][key] = value
		else:
			joined[root_key][key] = joinDicts(joined[root_key][key], value)
	
	aggregated = [
		(r, json.loads(k) if k else [], v)
		for r in joined
		for k, v in joined[r].items()
	]
	
	return aggregated


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
		return ''.join([
			f"['{s}']" if type(s) == str else f"[{s}]"
			for s in self.path
		]) or '.'
	
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

		print('addToIndex', self, field)

		if not self.parent:
			return False
		
		if not self.parent or not self.parent.isIndexExists(field):
			print('not self.parent or not self.parent.isIndex.Exists')
			return False

		if value == None:

			value = self[field]()
			if not value:
				print(f"not value: self['{field}'] == {value}")
				return False
		
		index = self.parent.getIndex(field)
		(index[value][self.path[-1]]).set(True, temp)

		return True
	
	def removeFromIndex(self, field, pipeline=None):

		db = pipeline or self.db

		if not self.parent or not self.parent.isIndexExists(field):
			return False
		
		index = self.parent.getIndex(field)
		
		value = self[field]()
		if not value:
			return
		
		index[value].__delitem__(self.path[-1], db)
		if not len(index[value]):
			index.__delitem__(value, db)
	
	def addToIndexes(self, payload, temp=None):

		print('addToIndexes', self, payload)

		if not hasattr(self, 'indexes'):
			return

		if type(payload) == dict:
			for k in payload.keys():
				self[k].addToIndexes(payload[k], temp)
		else:
			if self.parent:
				self.parent.addToIndex(self.path[-1], temp, value=payload)
	
	def removeFromIndexes(self, pipeline=None):
		
		if not hasattr(self, 'indexes'):
			return
		
		if self.type == 'object':
			for k in self.keys():
				self[k].removeFromIndexes(pipeline)
		else:
			if self.parent:
				self.parent.removeFromIndex(self.path[-1], pipeline)
	
	def createIndex(self, field):

		if self.isIndexExists(field):
			return
		
		index = self.getIndex(field)
		index.set({})

		temp = []
		for e in self:
			e.addToIndex(field, temp)
		
		self.makeSetsCalls(temp)
	
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
	
	def composeCorrectSetCall(self, value):
	
		for i in range(len(self.path)):
				
			path_ = self.path[:i]

			r = RedisInterface(self.db, path_, root_key=self.root_key)
			if r.type not in ['object', 'array']:
				
				for j in reversed(range(i, len(self.path))):
					if type(self.path[j]) != int:
						value = {
							self.path[j]: value
						}
					else:
						value = [value]
				
				return (self.root_key, path_, value)
		
		return (self.root_key, self.path, value)
	
	def makeSetsCalls(self, calls):

		print('makeSetsCalls', json.dumps(calls, indent=4))

		def transaction_function(pipe):

			print('transaction_function')

			for i, c in enumerate(calls):
				
				root_key, path, value = c
				r = RedisInterface(self.db, path, root_key=root_key)
				
				if root_key != 'index':
					r.addToIndexes(value, calls)
				
				calls[i] = r.composeCorrectSetCall(value)

			aggregated_calls = aggregateSetCalls(calls)
			print('aggregated_calls:', json.dumps(aggregated_calls, indent=4))
			
			pipe.multi()
			for root_key, path, value in aggregated_calls:
				_path = RedisInterface(self.db, path, root_key=self.root_key).ReJSON_path
				print('jsonset', _path, value)
				pipe.jsonset(root_key, _path, value)
			
		self.db.transaction(transaction_function, 'default')

	def set(self, value, temp=None):

		print('set', self, value, temp)

		new_call = (self.root_key, self.path, value)
		
		if temp != None:
			temp.append(new_call)
		else:
			self.makeSetsCalls([new_call])

	def __setitem__(self, key, value):

		if isinstance(value, RedisInterface):
			return

		self[key].set(value)
	
	def clear(self, pipeline=None):

		db = pipeline or self.db.pipeline()

		self.removeFromIndexes(db)
		db.jsondel(self.root_key, self._path)

		if not pipeline:
			db.execute()

	def __delitem__(self, key, pipeline=None):
		self[key].clear(pipeline)
	
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


import sys
sys.modules[__name__] = RedisInterface