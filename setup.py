import sys
import os
import glob
from dl.__version__ import __version__ as dl_version
from setuptools import setup, find_packages

#from vos.__version__ import vos_version

#if sys.version_info[0] > 2:
#    print 'The vos package is only compatible with Python version 2.n'
#    sys.exit(-1)

## Build the list of scripts to be installed.
script_dir = 'scripts'
scripts = []
for script in os.listdir(script_dir):
    if script[-1] in [ "~", "#"]:
        continue
    scripts.append(os.path.join(script_dir,script))

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

import unittest
def my_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    return test_suite

setup(name="astro-datalab",
      version=dl_version,
      url="https://github.com/astro-datalab/datalab/",
      description="Tools for interacting with NOIRLab's Astro Data Lab.",
      author="M.J. Graham, M.J. Fitzpatrick, D.L. Nidever, R. Nikutta",
      author_email="mjg@caltech.edu, mike.fitzpatrick@noirlab.edu, robert.nikutta@noirlab.edu",
      long_description="Tools for interacting with NOIRLab's Astro Data Lab services",
      packages=find_packages(exclude=['test.*']),
      package_data ={
        'datalab': ['caps/*']
      },
      scripts=['scripts/datalab', 'scripts/mountvofs'],
      test_suite='setup.my_test_suite',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Science/Research',
          'License :: OSI Approved :: MIT License',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Topic :: Scientific/Engineering :: Astronomy',
        ], 
      install_requires=requirements,
      data_files=[('', ['requirements.txt'])],
      )
