import pandas as pd
from sqlalchemy import create_engine

db_params = {
    'host': 'daea_semana10_cambios-db-1',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'port': 5432
}

engine = create_engine(f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')

df1 = pd.read_csv('movies.csv')
df2 = pd.read_csv('ratings.csv')

create_movies = """
CREATE TABLE movies (
    movieId SERIAL PRIMARY KEY,
    title TEXT,
    genres TEXT
);
"""

create_ratings = """
CREATE TABLE ratings (
    userId INTEGER,
    movieId INTEGER,
    rating NUMERIC,
    timestamp TIMESTAMP
);
"""

i_movies = df1.to_sql('movies', index=False, if_exists='replace', con=engine, schema=None, method='multi')
i_ratings = df2.to_sql('ratings', index=False, if_exists='replace', con=engine, schema=None, method='multi')

sql_script = f"{create_movies}\n{i_movies}\n{create_ratings}\n{i_ratings}"

with open('output.sql', 'w') as f:
    f.write(sql_script)