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

	print('aggregate', calls, method_name)

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

		for _, (root_key, path) in calls:

			current = joined
			for i, p in enumerate(path):

				if i == len(path)-1:
					current[p] = None
				
				else:
					current[p] = {}
					current = current[p]

		return [
			DeleteCall((method_name, (root_key, list(path)))) 
			for path in (flatten(joined) or [[]])
		]


class Calls(list):

	def aggregate(self):

		calls_by_method_name = {}

		for c in self:

			if not c.method_name in calls_by_method_name:
				calls_by_method_name[c.method_name] = []
			calls_by_method_name[c.method_name].append(c)
		
		result = []

		for name, calls in calls_by_method_name.items():
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



import sys
sys.modules[__name__] = Calls