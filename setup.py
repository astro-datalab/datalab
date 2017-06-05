
from distutils.core import setup
import sys
import os
from vos.__version__ import version

if sys.version_info[0] > 2:
    print 'The vos package is only compatible with Python version 2.n'
    sys.exit(-1)

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


setup(name="datalab-client",
      version="1.0.6",
      url="https://github.noao.edu/noao-datalab/datalab-client",
      description="Tools for interacting with NOAO Data Lab.",
      author="M.J. Graham, M.J. Fitzpatrick, D.L. Nidever, R. Nikutta",
      author_email="graham@noao.edu, fitz@noao.edu, dnidever@noao.edu, nikutta@noao.edu",
      long_description="Tools for interacting with the NOAO Data Lab services",
      packages=find_packages(exclude=['test.*']),
      package_data ={
        'datalab': ['caps/*']
      },
      scripts=['scripts/datalab', 'scripts/mountvofs'],
      classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Affero General Public License v3',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        ], 
      install_requires=['requests>=2.7', 'argparse', 'lxml', 'httplib2', 'numpy', 'astropy', 'pyvo', 'matplotlib', 'shapely'],
      requires=['requests (>=2.7)', 'argparse', 'lxml', 'httplib2', 'numpy', 'astropy', 'pyvo', 'matplotlib', 'shapely']
      )
