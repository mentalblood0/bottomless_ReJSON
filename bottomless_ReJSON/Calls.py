import uuid
import json
from rejson import Client
from redis import WatchError
from flatten_dict import flatten
from deepmerge import always_merger

from .calls import *
from .common import *
from . import makeCaching


def aggregate(calls, method_name):

    if method_name == "jsonset":

        joined = {}

        for _, (root_key, path, value) in calls:

            key = json.dumps(path)

            if not root_key in joined:
                joined[root_key] = {}

            if not key in joined[root_key]:
                joined[root_key][key] = value
            else:
                joined[root_key][key] = always_merger.merge(
                    joined[root_key][key], value
                )

        aggregated = [
            SetCall((method_name, (r, json.loads(k) if k else [], v)))
            for r in joined
            for k, v in joined[r].items()
        ]

        return aggregated

    elif method_name == "jsondel":

        joined = {}
        result = []

        for _, (root_key, path) in calls:

            if not path:
                result.append(DeleteCall((method_name, (root_key, path))))

            current = joined
            for i, p in enumerate(path):

                if i == len(path) - 1:
                    current[p] = None

                else:
                    if not p in current or type(current[p]) != dict:
                        current[p] = {}
                    current = current[p]

        result += [
            DeleteCall((method_name, (root_key, list(path))))
            for path in flatten(joined)
        ]

        return result


class Calls(list):

    methods_order = ["jsondel", "jsonset"]

    def aggregate(self):

        calls_by_method_name = {k: [] for k in self.methods_order}
        for c in self:
            calls_by_method_name[c.method_name].append(c)

        result = []
        for name in self.methods_order:
            result.extend(aggregate(calls_by_method_name[name], name))

        return result

    def getPrepared(self, db):

        result = []

        for c in self:
            result.append(c)
            result.extend(c.getAdditionalCalls(db))

        return Calls(map(lambda c: c.getCorrect(db), result)).aggregate()

    def __call__(self, db):

        host = db.connection_pool.connection_kwargs["host"]
        port = db.connection_pool.connection_kwargs["port"]
        db_caching = Client(host=host, port=port, decode_responses=True)
        makeCaching(
            db_caching,
            [
                "jsonget",
                "jsonmget",
                "jsontype",
                "jsonobjkeys",
                "jsonarrindex",
                "jsonarrlen",
                "jsonobjlen",
                "jsonstrlen",
            ],
        )

        prepared_calls = None

        while True:

            try:

                db_caching._cache = {}
                prepared_calls = prepared_calls or self.getPrepared(db_caching)

                if len(prepared_calls):

                    id_keys = [
                        f"transaction_{c.root_key}{c.path}" for c in prepared_calls
                    ]

                    pipe = db.pipeline()
                    pipe.watch(*id_keys)

                    db_caching._cache = {}
                    again_prepared_calls = self.getPrepared(db_caching)
                    if again_prepared_calls != prepared_calls:
                        prepared_calls = again_prepared_calls
                        raise WatchError

                    pipe.multi()

                    transaction_id = uuid.uuid4().hex
                    pipe.mset({k: transaction_id for k in id_keys})
                    for c in prepared_calls:
                        c(pipe)

                    pipe.execute()
                    db.delete(*id_keys)

            except WatchError:
                continue

            break


import sys

sys.modules[__name__] = Calls
