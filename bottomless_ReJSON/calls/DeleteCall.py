from functools import cached_property

from .. import Call
from ..common import *
from .. import RedisInterface



class DeleteCall(Call):

	@property
	def root_key(self):
		return self.args[0]

	@property
	def path(self):
		return self.args[1]

	def getCorrect(self, *args, **kwargs):
		return DeleteCall((self.method_name, (self.root_key, self.path)))
	
	def getAdditionalCalls(self, db):

		if self.root_key == 'index':
			return []
		
		indexes_calls = []
		
		r = RedisInterface.RedisInterface(db, self.path, root_key=self.root_key)
		r.removeFromIndexes(indexes_calls)
		
		return indexes_calls
	
	@cached_property
	def prepared_args(self):
		return (self.root_key, composeRejsonPath(self.path))



import sys
sys.modules[__name__] = DeleteCall