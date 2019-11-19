"""
(This is a file-level docstring.)
This file contains code for Flask server that handles API requests
"""
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import query

app = Flask(__name__)
CORS(app)


@app.route('/')
def cover():
    return send_file('cover.html')


@app.route('/cover.css')
def send_cover_css():
    return send_file('cover.css')


@app.route('/visualization.js')
def send_visualization_js():
    return send_file('visualization.js')


@app.route('/index.html')
def index():
    return send_file('index.html')


@app.route("/query1")
def query1():
    minFare = int(request.args.get('minFare'))
    maxFare = int(request.args.get('maxFare'))

    result = query.query1(minFare, maxFare)
    return jsonify(result)


@app.route("/query2")
def query2():
    search = request.args.get('search')
    minReviews = int(request.args.get('minReviews'))

    result = query.query2(search, minReviews)
    return jsonify(result)


@app.route("/query3")
def query3():
    result = query.query3()
    return jsonify(result)


@app.route("/query4")
def query4():
    result = query.query4()
    return jsonify(result)


@app.route("/query5")
def query5():
    result = query.query5()
    return jsonify(result)


if __name__ == "__main__":
    app.run("0.0.0.0", port=5000, debug=True)
