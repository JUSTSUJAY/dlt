# import dlt and create a pipeline
import dlt 
pipeline = dlt.pipeline(pipeline_name = 'pokemon', destination = 'duckdb', dataset_name = 'pokemon_data_1')

# Get the data from the source
from dlt.sources.helpers import requests
POKEMON_URL = "https://pokeapi.co/api/v2/pokemon/"
data = requests.get(POKEMON_URL).json()['results']
# print(type(data)) list
# print(len(data)) 20

# Load info
load_info = pipeline.run(data, table_name = 'pokemon')
print(load_info)

# load the data in the pipeline
import duckdb
conn = duckdb.connect('pokemon.duckdb')
tables = conn.execute('SELECT * FROM information_schema.tables').fetchall()
print(tables)
result = conn.execute('SELECT * FROM pokemon.pokemon_data_1.pokemon LIMIT 10').fetchall()
print(result)
conn.close()