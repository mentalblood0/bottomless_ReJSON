import json



def getCached(object, method):

	name = method.__name__

	def cached(*args, **kwargs):

		if not name in object._cache:
			object._cache[name] = {}
		
		key = json.dumps([args, kwargs])
		if not key in object._cache[name]:
			object._cache[name][key] = method(*args, **kwargs)
		
		return object._cache[name][key]

	return cached


def makeCaching(object, methods_names):

	object._cache = {}

	for name in methods_names:
		
		if hasattr(object, name):

			method = getattr(object, name)
			new_method = getCached(object, method)
			setattr(object, name, new_method)



import sys
sys.modules[__name__] = makeCaching