import uuid
import json
from loguru import logger
from rejson import Client
from redis import WatchError
from flatten_dict import flatten

from .calls import *
from .common import *
from . import Call, makeCaching


def joinDicts(*args):

	result = {}

	for d in args:
		
		for key in d:
			if key in result:
				result[key] = joinDicts(result[key], d[key])
			else:
				result[key] = d[key]
	
	return result


def aggregate(calls, method_name):

	if method_name == 'jsonset':

		joined = {}

		for _, (root_key, path, value) in calls:
			
			key = json.dumps(path)
			
			if not root_key in joined:
				joined[root_key] = {}
			
			if not key in joined[root_key]:
				joined[root_key][key] = value
			else:
				joined[root_key][key] = joinDicts(joined[root_key][key], value)
		
		aggregated = [
			SetCall((method_name, (r, json.loads(k) if k else [], v)))
			for r in joined
			for k, v in joined[r].items()
		]
		
		return aggregated
	
	elif method_name == 'jsondel':

		joined = {}
		result = []

		for _, (root_key, path) in calls:

			if not path:
				result.append(DeleteCall((method_name, (root_key, path))))

			current = joined
			for i, p in enumerate(path):

				if i == len(path)-1:
					current[p] = None
				
				else:
					if not p in current or type(current[p]) != dict:
						current[p] = {}
					current = current[p]

		result += [
			DeleteCall((method_name, (root_key, list(path)))) 
			for path in flatten(joined)
		]
		
		return result


class Calls(list):

	methods_order = [
		'jsondel',
		'jsonset'
	]

	def aggregate(self):

		calls_by_method_name = {}

		for c in self:

			if not c.method_name in calls_by_method_name:
				calls_by_method_name[c.method_name] = []
			calls_by_method_name[c.method_name].append(c)
		
		result = []

		for name in self.methods_order:

			if not name in calls_by_method_name:
				continue
			
			calls = calls_by_method_name[name]
			result.extend(aggregate(calls, name))
		
		return result
	
	def getPrepared(self, db):

		result = Calls([])

		for c in self:
			result.append(c)
			result.extend(c.getAdditionalCalls(db))
		
		for i, c in enumerate(result):
			result[i] = c.getCorrect(db)

		return result.aggregate()
	
	def __call__(self, db):

		id_value = uuid.uuid4().hex
		logger.add(f"log/{id_value}", filter=lambda r: id_value in r['extra'])
		local_logger = logger.bind(**{id_value: True})

		host = db.connection_pool.connection_kwargs['host']
		port = db.connection_pool.connection_kwargs['port']
		db_caching = Client(host=host, port=port, decode_responses=True)
		makeCaching(
			db_caching, [
				'jsonget', 
				'jsonmget',
				'jsontype',
				'jsonobjkeys',
				'jsonarrindex',
				'jsonarrlen',
				'jsonobjlen',
				'jsonstrlen'
			]
		)

		prepared_calls = None
		id_keys = None

		while True:

			try:
				
				db_caching._cache = {}
				prepared_calls = self.getPrepared(db_caching)
				id_keys = [f"transaction_{c.root_key}{composeRejsonPath(c.path)}" for c in prepared_calls]
				local_logger.warning(f"prepared_calls {json.dumps(prepared_calls, indent=4)}\nid_keys: {json.dumps(id_keys, indent=4)}")

				if len(id_keys):
					
					pipe = db.pipeline()
					while True:
						try:
							pipe.watch(*id_keys)
							pipe.multi()
							if id_keys:
								pipe.mset({
									k: id_value
									for k in id_keys
								})
							for c in prepared_calls:
								c(pipe)
							pipe.execute()
							# db.delete(*id_keys)
							break
						except WatchError:
							raise
						finally:
							pipe.reset()
					
				else:
					
					pipe = db.pipeline()
					for c in prepared_calls:
						c(pipe)
					pipe.execute()

				break

			except WatchError:
				local_logger.warning('WATCH ERROR')
				continue
		
		local_logger.warning(f"FINAL prepared_calls {json.dumps(prepared_calls, indent=4)}\nid_keys: {json.dumps(id_keys, indent=4)}")



import sys
sys.modules[__name__] = Calls