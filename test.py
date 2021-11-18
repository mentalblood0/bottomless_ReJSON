from redis import ResponseError
import bottomless_ReJSON.RedisInterface as RedisInterface
from redisearch import Client, IndexDefinition, TextField, IndexType



SCHEMA = (
    TextField("title"),
    TextField("body")
)

client = Client("my-index", host='10.8.5.170', port='6378')

definition = IndexDefinition(index_type=IndexType.JSON)

try:
    client.info()
except ResponseError:
    # Index does not exist. We need to create it!
    client.create_index(SCHEMA, definition=definition)