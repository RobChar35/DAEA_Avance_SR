from flask import Flask, render_template, request, make_response, g, jsonify
from redis import Redis
import os
import socket
import random
import json
import logging
from math import sqrt

import psycopg2

from collections import defaultdict

option_a = os.getenv('OPTION_A', "Manhatan")
option_b = os.getenv('OPTION_B', "Pearson")
hostname = socket.gethostname()

def manhattan(rating1, rating2):
    distance = 0
    total = 0
    for key in rating1:
        if key in rating2:
            distance += abs(rating1[key] - rating2[key])
            total += 1
    if total > 0:
        return distance / total
    else:
        return -1 #Indicates no ratings in common

def pearson(rating1, rating2):
    sum_xy = 0
    sum_x = 0
    sum_y = 0
    sum_x2 = 0
    sum_y2 = 0
    n = 0
    for key in rating1:
        if key in rating2:
            n += 1
            x = rating1[key]
            y = rating2[key]
            sum_xy += x * y
            sum_x += x
            sum_y += y
            sum_x2 += pow(x, 2)
            sum_y2 += pow(y, 2)
    # now compute denominator
    denominator = sqrt(sum_x2 - pow(sum_x, 2) / n) * sqrt(sum_y2 - pow(sum_y, 2) / n)
    if denominator == 0:
        return 0
    else:
            return (sum_xy - (sum_x * sum_y) / n) / denominator


app = Flask(__name__)

gunicorn_error_logger = logging.getLogger('gunicorn.error')
app.logger.handlers.extend(gunicorn_error_logger.handlers)
app.logger.setLevel(logging.INFO)

def get_redis():
    if not hasattr(g, 'redis'):
        g.redis = Redis(host="redis", db=0, socket_timeout=5)
    return g.redis

# Datos de los usuarios
users = {"Angelica": {"Blues Traveler": 3.5, "Broken Bells": 2.0, "Norah Jones": 4.5, "Phoenix": 5.0, "Slightly Stoopid": 1.5,
"The Strokes": 2.5, "Vampire Weekend": 2.0},
"Bill": {"Blues Traveler": 2.0, "Broken Bells": 3.5,
"Deadmau5": 4.0, "Phoenix": 2.0,
"Slightly Stoopid": 3.5, "Vampire Weekend": 3.0},
"Chan": {"Blues Traveler": 5.0, "Broken Bells": 1.0,
"Deadmau5": 1.0, "Norah Jones": 3.0,
"Phoenix": 5, "Slightly Stoopid": 1.0},
"Dan": {"Blues Traveler": 3.0, "Broken Bells": 4.0,
"Deadmau5": 4.5, "Phoenix": 3.0,
"Slightly Stoopid": 4.5, "The Strokes": 4.0,
"Vampire Weekend": 2.0},
"Hailey": {"Broken Bells": 4.0, "Deadmau5": 1.0,
"Norah Jones": 4.0, "The Strokes": 4.0,
"Vampire Weekend": 1.0},
"Jordyn": {"Broken Bells": 4.5, "Deadmau5": 4.0, "Norah Jones": 5.0,
"Phoenix": 5.0, "Slightly Stoopid": 4.5,
"The Strokes": 4.0, "Vampire Weekend": 4.0},
"Sam": {"Blues Traveler": 5.0, "Broken Bells": 2.0,
"Norah Jones": 3.0, "Phoenix": 5.0,
"Slightly Stoopid": 4.0, "The Strokes": 5.0},
"Veronica": {"Blues Traveler": 3.0, "Norah Jones": 5.0,
"Phoenix": 4.0, "Slightly Stoopid": 2.5,
"The Strokes": 3.0}}

def obtener_valores():
    # Crear un diccionario para almacenar el conteo de calificaciones
    rating_counts = defaultdict(int)

    # Iterar a través de los usuarios y sus calificaciones
    for user_ratings in users.values():
        for rating in user_ratings.values():
            # Incrementar el conteo para la calificación actual
            rating_counts[rating] += 1

    # Guardar el resultado en un archivo JSON
    with open('rating_counts.json', 'w') as json_file:
        json.dump(rating_counts, json_file)

    return json.dumps(rating_counts)

@app.route("/", methods=['POST','GET'])
def distancias():
    voter_id = request.cookies.get('voter_id')
    
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]
    vote = None
    if request.method == 'POST':
        redis = get_redis()
        user_1 = request.form['option_a']
        user_2 = request.form['option_b']
        distancia_pearson = str(pearson(users[user_1], users[user_2]))
        distancia_manhattan = str(manhattan(users[user_1], users[user_2])) 
        # data = json.dumps({'voter_id': voter_id,'distancia_manhattan': distancia_manhattan, 'distancia_pearson': distancia_pearson})
        data = obtener_valores()
        redis.rpush('distancias', data)
    resp = make_response(render_template(
        'index.html',
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
    ))
    resp.set_cookie('voter_id', voter_id)
    return resp

db_params = {
    'host': 'daea_semana10_cambios-db-1',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}

def execute_query(query):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    conn.close()
    return cursor, data

def contar_calificaciones(json_data):
    # Crear un diccionario para almacenar el conteo de calificaciones
    rating_counts = defaultdict(int)

    # Iterar sobre cada entrada en el JSON
    for entrada in json_data:
        rating = entrada["rating"]
        # Incrementar el conteo para la calificación actual
        rating_counts[rating] += 1

    return rating_counts

@app.route("/testingdata", methods=['GET'])
def testing_new_data():
    voter_id = request.cookies.get('voter_id')
    
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]
    vote = None
    
    query = "SELECT * FROM ratings LIMIT 10;"
    
    cursor, result = execute_query(query)

    keys = [desc[0] for desc in cursor.description]
    data = [dict(zip(keys, row)) for row in result]

    resultado = contar_calificaciones(data)

    redis = get_redis()
    data = json.dumps({'voter_id': voter_id, 'conteo': jsonify(resultado)})

    return data

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80, debug=True, threaded=True)
