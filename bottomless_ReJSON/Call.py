class Call(tuple):

	@property
	def method_name(self):
		return self[0]
	
	@property
	def args(self):
		return self[1]

	def getCorrect(self, db):
		return self

	def getAdditionalCalls(self, db):
		return []
	
	def getPreparedArgs(self):
		return self.args
	
	def __call__(self, pipe):
		return getattr(pipe, self.method_name)(*self.getPreparedArgs())



import sys
sys.modules[__name__] = Call