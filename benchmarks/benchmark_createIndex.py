from sharpener import Benchmark

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



class same(Benchmark):

	def prepare(self, items_number):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.clear()
		self.interface['sessions'] = {
			str(i): {'state': 'new'}
			for i in range(items_number)
		}

	def run(self, **kwargs):
		self.interface['sessions'].createIndex('state')
	
	def clean(self, **kwargs):
		self.interface.clear()

class different(Benchmark):

	def prepare(self, items_number):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.clear()
		self.interface['sessions'] = {
			str(i): {'state': str(i)}
			for i in range(items_number)
		}

	def run(self, **kwargs):
		self.interface['sessions'].createIndex('state')
	
	def clean(self, **kwargs):
		self.interface.clear()