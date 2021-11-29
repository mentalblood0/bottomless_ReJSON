def composeRejsonPath(path):

	parts = []

	for s in path:

		if type(s) in [str, bool]:
			part = f"['{s}']"
		else:
			part = f"[{s}]"
		
		parts.append(part)

	return ''.join(parts) or '.'