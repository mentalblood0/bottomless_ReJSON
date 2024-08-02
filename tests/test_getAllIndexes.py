from tests import config
from bottomless_ReJSON import RedisInterface


def test_basic():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()

    interface["sessions"] = {
        "a": {"state": "new"},
        "b": {"state": "processed"},
        "c": {"state": "new"},
        "d": {"state": "processed"},
        "e": {"state": "erroneous"},
    }

    interface["sessions"].createIndex("state")

    assert interface.indexes_list == [["sessions", "state"]]
