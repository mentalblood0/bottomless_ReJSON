from .. import Call
from ..common import *
from .. import RedisInterface


class SetCall(Call):

	@property
	def root_key(self):
		return self.args[0]

	@property
	def path(self):
		return self.args[1]
	
	@property
	def value(self):
		return self.args[2]

	def getCorrect(self, db):

		for i in range(len(self.path)):
				
			path_ = self.path[:i]

			t = db.jsontype(self.root_key, composeRejsonPath(path_))
			if t not in ['object', 'array']:
				
				value = self.value
				for j in reversed(range(i, len(self.path))):
					if type(self.path[j]) != int:
						value = {
							self.path[j]: value
						}
					else:
						value = [value]
				
				return SetCall((self.method_name, (self.root_key, path_, value)))
		
		return SetCall((self.method_name, (self.root_key, self.path, self.value)))
	
	def getAdditionalCalls(self, db):

		if self.root_key == 'index':
			return []
		
		indexes_calls = []
		
		r = RedisInterface.RedisInterface(db, self.path, root_key=self.root_key)
		r.removeFromIndexes(indexes_calls)
		r.addToIndexes(self.value, indexes_calls)
		
		return indexes_calls
	
	def getPreparedArgs(self):
		return (self.root_key, composeRejsonPath(self.path), self.value)



import sys
sys.modules[__name__] = SetCall