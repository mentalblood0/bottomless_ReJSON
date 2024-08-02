import uuid
from time import sleep
from threading import Thread

from tests import config
from bottomless_ReJSON import RedisInterface


def test_interrupt():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    db = interface.db

    key = "default"
    value = uuid.uuid4().hex

    def transaction_function(pipe):

        if pipe.get(key) != value:
            pipe.set(key, value)

        pipe.multi()
        pipe.jsonset("root", ".", {"a": 1})

    db.transaction(transaction_function, key)
    assert db.get(key) == value


def test_delete_after_multi():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    db = interface.db

    key = "default"

    db.set("del", "lalala")

    def transaction_function(pipe):
        pipe.multi()
        pipe.delete("del")

    db.transaction(transaction_function, key)
    assert db.get("del") == None


def test_update():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    results = {"long_set": False, "long_get": None}

    def long_set(interface, key, value):
        interface[key] = value
        results["long_set"] = True

    def long_get(interface, key, valid_key, results):
        while not results["long_set"]:
            sleep(0.1)
        results["long_get"] = interface[key]() == valid_key

    n = 1 * 10**2
    key = "key"
    value = {str(i + 1): i + 1 for i in range(n)}

    setter = Thread(target=long_set, args=[interface, key, value])

    getter = Thread(target=long_get, args=[interface[key], str(n), n, results])

    setter.start()
    getter.start()

    setter.join()
    getter.join()

    assert results["long_get"] == True


def test_add():

    interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
    interface.clear()
    results = {"long_set": 0}

    def long_set(interface, value):
        interface.set(value)
        results["long_set"] += 1

    n = 1 * 10**2
    value = {
        "id": "35302f1ef45e42beae445d96ab5a5fb8",
        "name": "test_session",
        "type": "any",
        "opened": 1,
        "datetime": "2021-11-23 13:38:44.641060",
        "state": "new",
        "commandCount": 0,
    }

    setters = [Thread(target=long_set, args=[interface["sessions"][str(i)], value]) for i in range(n)]

    for s in setters:
        s.start()
        # s.join()

    while not (results["long_set"] == n):
        sleep(0.1)

    assert len(interface["sessions"]) == n
