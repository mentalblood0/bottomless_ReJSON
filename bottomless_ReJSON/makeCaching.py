import json



def getFromCache(object, name, key):
	return object._cache[name][key]


def addToCache(object, name, key, value):
	object._cache[name][key] = value


def getCached(object, method):

	name = method.__name__

	def cached(*args, **kwargs):

		if not name in object._cache:
			object._cache[name] = {}
		
		key = json.dumps([args, kwargs])
		if not key in object._cache[name]:
			result = method(*args, **kwargs)
			addToCache(object, name, key, result)
		else:
			result = getFromCache(object, name, key)
		
		return result

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