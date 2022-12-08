from setuptools import setup, find_packages
import os


def readme():
    with open("README.md") as ff:
        return ff.read()


thelibFolder = os.path.dirname(os.path.realpath(__file__))
requirementPath = thelibFolder + '/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()

fileversion = thelibFolder + "/gdsbbta/__init__.py"
with open(fileversion) as f:
    versionnumber = f.read()
    versionnumber = versionnumber[versionnumber.find("'")+1:-2]

setup(name="gdsbbta",
      version=versionnumber,
      description="python GDS Bank Attributes",
      long_description=readme(),
      classifiers=['Development Status:: Production',
                   'Programming Language:: Python::3.8',
                   'License:: None'],
      keywords='',
      author='gdslink',
      author_email='florian.lyonnet@gdslink.com',
      install_requires=install_requires,
      packages=find_packages(),
      package_data={
          'gdsbbta.engine.examples': ["*"],
          'gdsbbta.merger': ["dictionaries/*"],
          'gdsbbta.models': ["short_term_subprime/v1.0.0/*"]
      }
      )
