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



import sys
sys.modules[__name__] = Call