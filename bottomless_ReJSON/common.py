def composeRejsonPath(path):
	return ''.join([
		f"['{s}']" if type(s) == str else f"[{s}]"
		for s in path
	]) or '.'