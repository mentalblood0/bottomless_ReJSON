from sharpener import Benchmark

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



class in_object(Benchmark):

	def prepare(self, items_number):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.clear()
		self.interface['sessions'] = {
			str(i): {}
			for i in range(items_number)
		}

	def run(self, items_number):
		str(items_number // 2) in self.interface
	
	def clean(self, **kwargs):
		self.interface.clear()