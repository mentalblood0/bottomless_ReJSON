import uuid
from datetime import datetime
from sharpener import Benchmark

from tests import config
from bottomless_ReJSON import RedisInterface



class simple(Benchmark):

	def prepare(self, **kwargs):

		self.interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.interface.clear()
	
	def run(self, keys_number):
		self.interface |= {
			i+1: i+1
			for i in range(keys_number)
		}
	
	def clean(self, **kwargs):
		self.interface.clear()


class realistic(Benchmark):

	def prepare(self, **kwargs):

		interface = RedisInterface(host=config['db']['host'], port=config['db']['port'])
		self.sessions = interface['sessions']

	def run(self, keys_number):

		for i in range(keys_number):
		
			while True:
				id = uuid.uuid4().hex
				if not id in self.sessions:
					break

			self.sessions[id] = {
				'id': id,
				'name': i,
				'opened': 1,
				'datetime': str(datetime.now()), 
				'state': 'new',
				'commandCount': 0
			}
	
	def clean(self, **kwargs):
		self.sessions.clear()