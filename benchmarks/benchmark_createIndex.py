from sharpener import Benchmark

from tests import config
from bottomless_ReJSON import RedisInterface



class same(Benchmark):

	def prepare(self, items_number):

		self.interface = RedisInterface.RedisInterface(host=config['db']['host'], port=config['db']['port'])
	interface.indexes.clear()
	interface.clear()
	interface.clear()
		self.interface.indexes.clear()
		self.interface.clear()

		self.interface['sessions'] = {
			str(i): {'state': 'new'}
			for i in range(items_number)
		}

	def run(self, **kwargs):
		self.interface['sessions'].createIndex('state')
	
	def clean(self, **kwargs):
		self.interface.indexes.clear()
		self.interface.clear()