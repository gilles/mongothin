# coding=utf-8
from distutils.core import setup

setup(
    name='mongothin',
    version='0.1',
    packages=[''],
    url='https://github.com/gilles/mongothin',
    license='MIT',
    author='Gilles Devaux',
    author_email='gilles.devaux@gmail.com',
    description='Thin helper around pymongo',
    keywords=["mongo", "mongodb", "pymongo"],
    tests_require=["nose"],
    install_requires=["pymongo>=2.4"],
    packages=["mongothin"],
    test_suite="nose.collector"
)
