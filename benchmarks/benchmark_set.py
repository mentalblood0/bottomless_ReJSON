from sharpener import Benchmark
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

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


class without_index_descrete(Benchmark):

	def prepare(self, **kwargs):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()

	def run(self, items_number):
		for i in range(items_number):
			self.interface['sessions'][str(i)] = {'state': 'new'}
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()


class with_index(Benchmark):

	def prepare(self, **kwargs):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()
		self.interface['sessions'].createIndex('state')
		self.interface.use_indexes_cache = False
		self.interface.use_indexes_cache = True

	def run(self, items_number):
		self.interface['sessions'] = {
			str(i): {'state': 'new'}
			for i in range(items_number)
		}
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()


class with_index_descrete(Benchmark):

	def prepare(self, **kwargs):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.db.flushdb()
		self.interface['sessions'].createIndex('state')
		self.interface.use_indexes_cache = False
		self.interface.use_indexes_cache = True

	def run(self, items_number):
		for i in range(items_number):
			self.interface['sessions'][str(i)] = {'state': 'new'}
	
	def clean(self, **kwargs):
		self.interface.db.flushdb()