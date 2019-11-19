"""
(This is a file-level docstring.)
This file drops all collections from MongoDb 'test' database.
"""
from pymongo import MongoClient


def restartMongo():
    """ Drops all collections from MongoDb 'test' database. Useful for testing etl.py
    """
    db = MongoClient().test
    db.dropDatabase()


if __name__ == "__main__":
    restartMongo()
