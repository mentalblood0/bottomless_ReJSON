import os
from distutils.core import setup



if __name__ == '__main__':

	long_description = ''
	if os.path.exists('README.md'):
		with open('README.md', encoding='utf-8') as f:
			long_description = f.read()

	setup(
		name='bottomless_ReJSON',
		version='0.5',
		description='Correct Redis Library',
		long_description=long_description,
		long_description_content_type='text/markdown',
		author='mentalblood',
		install_requires=[
			'rejson',
			'flatten_dict'
		],
		packages=['bottomless_ReJSON', 'bottomless_ReJSON.calls']
	)