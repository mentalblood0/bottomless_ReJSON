import json

from tests import config
from bottomless_ReJSON import RedisInterface


def test_valid_key():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    interface["key"] = "value"

    del interface["key"]


def test_cascade():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()

    interface["1"] = "one"
    interface["2"] = "two"
    interface["1"]["1"] = "one.one"
    interface["1"]["2"] = "one.two"
    interface["11"] = "oneone"

    del interface["1"]

    assert interface["1"]() == None
    assert interface["1"] == None
    assert interface["1"]["1"] == None
    assert interface["1"]["2"] == None
    assert interface["11"] == "oneone"

    del interface["2"]

    assert interface["2"] == None


def test_with_indexes():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()

    with open("tests/test_delete_db.json") as f:
        db = json.load(f)
    interface.set(db)

    with open("tests/test_delete_db_indexes.json") as f:
        indexes = json.load(f)
    interface.indexes.set(indexes)
    interface.updateIndexesList()

    assert interface == db
    assert interface.indexes == indexes
    sessions_ids = set(interface["sessions"].keys())
    sessions_ids_in_index = set(
        interface.indexes["sessions"]["__index__"]["state"]["finished"].keys()
    )
    assert sessions_ids == sessions_ids_in_index

    interface.clear()

    assert interface == None
    assert interface.indexes == None
