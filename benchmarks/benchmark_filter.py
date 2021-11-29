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
		self.interface['sessions'][str(items_number // 2)]['state'] = 'new'

	def run(self, **kwargs):
		self.interface['sessions'].filter(state='new')
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()


class n_index(Benchmark):

	def prepare(self, items_number, index_number):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()

		for i in range(index_number):
			self.interface['sessions'].createIndex(f"property_{i}")
		self.interface.use_indexes_cache = False
		self.interface.use_indexes_cache = True

		self.interface['sessions'] = {
			str(j): {
				f"property_{i}": bool(j)
				for i in range(index_number)
			}
			for j in range(items_number)
		}
		self.interface['sessions'][str(items_number // 2)]['state'] = 'new'

	def run(self, index_number, **kwargs):
		self.interface['sessions'].filter(**{
			f"property_{i}": True
			for i in range(index_number)
		})
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()