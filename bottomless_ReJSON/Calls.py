import json
from flatten_dict import flatten

from . import Call
from .calls import *



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
	
	# elif method_name == 'jsondel':

	# 	joined = {}

	# 	for root_key, path in calls:

	# 		current = joined
	# 		for p in path:
	# 			current[p] = {}
	# 			current = current[p]
		
	# 	return [(method_name, (root_key, list(path))) for path in flatten(joined)]


class Calls(list):

	def aggregate(self):

		print('aggregate', type(self), self)

		calls_by_method_name = {}

		for c in self:

			if not c.method_name in calls_by_method_name:
				calls_by_method_name[c.method_name] = []
			calls_by_method_name[c.method_name].append(c)
		
		result = []

		print('calls_by_method_name', calls_by_method_name)
		for name, calls in calls_by_method_name.items():
			print('for', result, calls, name)
			result.extend(aggregate(calls, name))
		
		return result
	
	def getPrepared(self, db):

		result = []

		for c in self:
			result.append(c.getCorrect(db))
			result.extend(c.getAdditionalCalls(db))

		return result



import sys
sys.modules[__name__] = Calls