from sharpener import Benchmark

from tests import config
import bottomless_ReJSON.RedisInterface as RedisInterface



class one_index(Benchmark):

	def prepare(self, items_number):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()
		self.interface['sessions'].createIndex('state')
		self.interface.use_indexes_cache = False
		self.interface.use_indexes_cache = True

		self.interface['sessions'] = {
			str(i): {'state': 'finished'}
			for i in range(items_number)
		}
		self.interface['sessions'][items_number // 2]['state'] = 'new'

	def run(self, **kwargs):
		self.interface['sessions'].filter(state='new')
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()