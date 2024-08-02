from sharpener import Benchmark

from tests import config
from bottomless_ReJSON import RedisInterface


class without_index(Benchmark):

    def prepare(self, items_number):

        self.interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
        self.interface.clear()
        self.interface.updateIndexesList()

        self.interface["sessions"] = {str(i): {"state": "new"} for i in range(items_number)}

    def run(self, **kwargs):
        self.interface.clear()

    def clean(self, **kwargs):
        self.interface.clear()


class with_index(Benchmark):

    def prepare(self, items_number):

        self.interface = RedisInterface(host=config["db"]["host"], port=config["db"]["port"])
        self.interface.clear()
        self.interface.updateIndexesList()

        self.interface["sessions"].createIndex("state")
        self.interface.use_indexes_cache = False
        self.interface.use_indexes_cache = True

        self.interface["sessions"] = {str(i): {"state": "new"} for i in range(items_number)}

    def run(self, **kwargs):
        self.interface.clear()

    def clean(self, **kwargs):
        self.interface.clear()
