
from distutils.core import setup
import sys
import os
from dl.__version__ import version as dl_version
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

try:
    from setuptools import setup, find_packages
    has_setuptools = True
except:
    from distutils.core import setup
    has_setuptools = False

import unittest
def my_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
    return test_suite

setup(name="noaodatalab",
      version=dl_version,
      url="https://github.com/noaodatalab/datalab/",
      description="Tools for interacting with NOAO Data Lab.",
      author="M.J. Graham, M.J. Fitzpatrick, D.L. Nidever, R. Nikutta",
      author_email="graham@noao.edu, fitz@noao.edu, dnidever@noao.edu, nikutta@noao.edu",
      long_description="Tools for interacting with the NOAO Data Lab services",
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
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Topic :: Scientific/Engineering :: Astronomy',
        ], 
      install_requires=['requests>=2.7', 'httplib2', 'numpy>=1.13', 'astropy', 'pyvo', 'matplotlib','pandas'],
      requires=['requests (>=2.7)', 'httplib2', 'numpy (>=1.13)', 'astropy', 'pyvo', 'matplotlib','pandas']
      )
