from distutils.core import setup
import sys
import os
import glob
from dl.__version__ import __version__ as dl_version
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


def read_requirements():
    # Determine the filename based on the Python version
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    requirements_filename = f"requirements_{python_version}.txt"

    # Read the requirements from the file
    try:
        with open(requirements_filename) as f:
            requirements = f.read().splitlines()
    except FileNotFoundError:
        # Find all available requirements files
        available_files = glob.glob('requirements_*.txt')

        # Extract available Python versions from filenames
        available_versions = [filename.split('_')[1].split('.txt')[0] for filename in available_files]

        # Sort versions for better readability
        available_versions.sort()

        # Raise an exception with information about supported Python versions
        raise RuntimeError(f"Requirements file {requirements_filename} not found. "
                           f"Supported Python versions are: {', '.join(available_versions)}.")


    return requirements

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
          'License :: OSI Approved :: GNU Affero General Public License v3',
          'Operating System :: POSIX',
          'Programming Language :: Python',
          'Topic :: Scientific/Engineering :: Astronomy',
        ], 
      install_requires=read_requirements()
      )
