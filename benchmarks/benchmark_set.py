from sharpener import Benchmark

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



class without_index(Benchmark):

	def prepare(self, **kwargs):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()

	def run(self, items_number):
		self.interface['sessions'] = {
			str(i): {'state': 'new'}
			for i in range(items_number)
		}
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()