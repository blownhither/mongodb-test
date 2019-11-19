"""
(This is a file-level docstring.)
This file contains all required queries to MongoDb.
"""
from pymongo import MongoClient

db = MongoClient().test


def query1(minFare, maxFare):
    """ Finds taxi rides with fare amount greater than or equal to minFare and less than or equal to maxFare.  

    Args:
        minFare: An int represeting the minimum fare
        maxFare: An int represeting the maximum fare

    Projection:
        pickup_longitude
        pickup_latitude
        fare_amount

    Returns:
        An array of documents.
    """
    docs = db.taxi.find(
        {
            'fare_amount': {
                '$gte': minFare,
                '$lte': maxFare
            }
        },
        {
            '_id': 0,
            'pickup_longitude': 1,
            'pickup_latitude': 1,
            'fare_amount': 1
        }
    )

    result = [doc for doc in docs]
    return result


def query2(textSearch, minReviews):
    """ Finds airbnbs with that match textSearch and have number of reviews greater than or equal to minReviews.  

    Args:
        textSearch: A str representing an arbitrary text search
        minReviews: An int represeting the minimum amount of reviews

    Projection:
        name
        number_of_reviews
        neighbourhood
        price
        location

    Returns:
        An array of documents.
    """
    docs = db.airbnb.find(
        {
            '$text': {
                '$search': textSearch
            },
            'number_of_reviews': {
                '$gte': minReviews
            }
        },
        {
            '_id': 0,
            'name': 1,
            'number_of_reviews': 1,
            'neighbourhood': 1,
            'price': 1,
            'location': 1
        }
    )

    result = [doc for doc in docs]
    return result


def query3():
    """ Groups airbnbs by neighbourhood_group and finds average price of each neighborhood_group sorted in descending order.  

    Returns:
        An array of documents.

    Example:
        tf = TFIDF.calc_term_frequency([
            ('Document A', ['the', 'student', 'eats', 'the', 'apple']),
            ('Document B', ['the', 'apple', 'is', 'red']),
        ])
        assert tf['student']['Document A'] == 1 / 5
    """
    docs = db.airbnb.aggregate([
        {
            '$group': {
                '_id': '$neighbourhood_group',
                'avgPrice': {
                    '$avg': '$price'
                }
            }
        },
        {
            '$sort': {'avgPrice': -1}
        }
    ])

    result = [doc for doc in docs]
    return result


def query4():
    """ Groups taxis by pickup hour. 
        Find average fare for each hour.
        Find average manhattan distance travelled for each hour.
        Count total number of rides per pickup hour.
        Sort by average fare in descending order.

    Returns:
        An array of documents.
    """
    docs = db.taxi.aggregate([
        {
            '$group': {
                '_id': {'$hour': "$pickup_datetime"},
                'avgFare': {'$avg': '$fare_amount'},
                'avgDistance': {
                    '$avg': {
                        '$add': [
                            {'$abs': {'$subtract': [
                                '$pickup_latitude', '$dropoff_latitude']}},
                            {'$abs': {'$subtract': [
                                '$pickup_longitude', '$dropoff_longitude']}}
                        ]
                    }
                },
                'count': {'$sum': 1}
            }
        },
        {
            '$sort': {'avgFare': -1}
        }
    ])
    result = [doc for doc in docs]
    return result


def query5():
    """ Finds airbnbs within 1000 meters from location (longitude, latitude) using geoNear. 
        Find average fare for each hour.
        Find average manhattan distance travelled for each hour.
        Count total number of rides per pickup hour.
        Sort by average fare in descending order.

    Projection:
        dist
        location
        name
        neighbourhood
        neighbourhood_group
        price
        room_type


    """
    docs = db.airbnb.aggregate([
        {
            '$geoNear': {
                'near': {'type': 'Point', 'coordinates': [-73.9654, 40.7829]},
                'distanceField': 'dist.calculated',
                'maxDistance': 1000,
                'includeLocs': 'dist.location',
                'spherical': False
            }
        },
        {
            '$project': {
                '_id': 0,
                'dist': 1,
                'location': 1,
                'name': 1,
                'neighbourhood': 1,
                'neighbourhood_group': 1,
                'price': 1,
                'room_type': 1
            }
        }
    ])
    result = [doc for doc in docs]
    return result