from setuptools import setup

setup(name='botowraps',
      version='0.1',
      description='Some wrappers around the boto package for dealing with AWS Services more easily. Currently has functions for S3 and Redshift.',
      url='https://github.com/calhank/botowraps',
      author='calhank',
      author_email='hank@notarealemailaddress.com',
      license='GPLv3',
      packages=[
            'botowraps'
      ],
      install_requires=[
	      'boto'
      ],
      zip_safe=False)
