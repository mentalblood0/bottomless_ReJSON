import copy
from functools import cached_property



class Call(tuple):

	@property
	def method_name(self):
		return self[0]
	
	@property
	def args(self):
		return self[1]
	
	@property
	def root_key(self):
		return self.args[0]

	@property
	def path(self):
		return self.args[1]

	def getCorrect(self, db):
		return copy.deepcopy(self)

	def getAdditionalCalls(self, db):
		return []
	
	@cached_property
	def prepared_args(self):
		return self.args
	
	def __call__(self, pipe):
		return getattr(pipe, self.method_name)(*self.prepared_args)



import sys
sys.modules[__name__] = Call