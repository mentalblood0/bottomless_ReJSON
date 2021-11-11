import redis
from rejson import Client, Path
from flatten_dict import flatten



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
	
	def _composeReJSONPath(self):
		return ''.join([
			f"['{s}']" if type(s) == str else f"[{s}]"
			for s in self.path
		]) or '.'
	
	@property
	def _path(self):
		return Path(self._composeReJSONPath())
	
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
	
	def addToIndex(self, field):

		if not self.parent.isIndexExists(field):
			return False

		index = self.parent.getIndex(field)

		value = self[field]()
		if not value:
			return False
		
		(index[value] + self.path).set(True)

		return True
	
	def removeFromIndex(self, field):

		if not self.parent.isIndexExists(field):
			return
		
		index = self.parent.getIndex(field)
		
		value = self[field]()
		if not value:
			return
		
		del (index[value] + self.path).parent[self.path[-1]]
	
	def createIndex(self, field):
		
		index = self.getIndex(field)
		index.set({})
		
		for e in self:
			e.addToIndex(field)
	
	def filter(self, field, value):

		if not self.isIndexExists(field):
			return [
				self[k]
				for k in self.keys()
				if self[k][field] == value
			]

		index = self.getIndex(field)

		paths_dict = index[value]() or {}
		paths = flatten(paths_dict, enumerate_types=(list,)).keys()

		return [
			RedisInterface(self.db, p, root_key=self.root_key)
			for p in paths
		]

	def set(self, value):

		for i in range(len(self.path)):
			
			path = self.path[:i]

			r = RedisInterface(self.db, path, root_key=self.root_key)
			if r.type != 'object':
				
				for j in reversed(range(i, len(self.path))):
					value = {
						self.path[j]: value
					}
				
				self.db.jsonset(self.root_key, r._path, value)
				return
		
		self.db.jsonset(self.root_key, self._path, value)

	def __setitem__(self, key, value):

		if isinstance(value, RedisInterface):
			return

		self[key].set(value)
	
	def clear(self):
		self.db.jsondel(self.root_key, self._path)

	def __delitem__(self, key):
		self[key].clear()
	
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
		return f"<{self.__class__.__name__} path=\"{self._composeReJSONPath()}\">"


import sys
sys.modules[__name__] = RedisInterface