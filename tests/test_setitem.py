import uuid
import time
from datetime import datetime
from threading import Thread

from tests import config
from bottomless_ReJSON import RedisInterface


def test_basic():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    interface["1"] = "one"
    assert interface["1"] == "one"

    interface["2"] = "two"
    interface["1"]["1"] = "one.one"
    interface["1"]["2"] = "one.two"

    assert interface[1] != "one"
    assert interface["2"] == "two"
    assert interface["1"]["1"] == "one.one"
    assert interface["1"]["2"] == "one.two"


def test_types():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    objects = [[], 1, "1" "str", {}]

    for o in objects:
        interface["key"] = o
        assert interface["key"] == o


def test_complex():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    interface["1"] = {"1": "one.one", "2": "one.two"}

    assert interface["1"]["1"] == "one.one"
    assert interface["1"]["2"] == "one.two"


def test_complex_arrays():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    interface["sessions"]["__index__"]["state"]["new"] = [
        ["sessions", "a"],
        ["sessions", "c"],
    ]


def test_complex_indexes_arrays():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    interface["sessions"] = {
        "a": {"state": "new"},
        "b": {"state": "processed"},
        "c": {"state": "new"},
        "d": {"state": "processed"},
        "e": {"state": "erroneous"},
    }

    interface.indexes["sessions"]["__index__"]["state"]["new"] = [
        ["sessions", "a"],
        ["sessions", "c"],
    ]


def test_many_complex():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    sessions = {}

    for i in range(2):

        existing_sessions_ids = interface["sessions"].keys()

        while True:
            id = uuid.uuid4().hex
            if not id in existing_sessions_ids:
                break

        new_session = {
            "id": id,
            "name": "name",
            "opened": 1,
            "datetime": str(datetime.now()),
            "state": "new",
            "commandCount": 0,
        }
        sessions[id] = new_session
        interface["sessions"][id] = new_session

    assert interface["sessions"] == sessions


def test_async():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    types = {bool: False, dict: {"a": 1, "b": 2}}

    report = {}

    def repeat_set(t):

        def f(interface, key, seconds, report):
            start = time.time()
            while start + seconds > time.time():
                interface[key] = types[t]
                report[str(key)] = types[t]

        return f

    seconds = 0.1
    keys_number = 1

    setters = {
        key: {
            t: Thread(target=repeat_set(t), args=[interface, key, seconds, report])
            for t in types
        }
        for key in range(keys_number)
    }

    for key in setters:
        for t in setters[key]:
            setters[key][t].start()

    start = time.time()
    while start + seconds > time.time():
        for key in setters:
            if key in report:
                result = interface[key]()
                if not any([result == value for value in types.values()]):
                    for key in setters:
                        for t in setters[key]:
                            setters[key][t].join()
                    assert False

    for key in setters:
        for t in setters[key]:
            setters[key][t].join()
