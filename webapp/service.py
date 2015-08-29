from functools import lru_cache
import time

import bottle
import datadog

from bottle.ext import sqlite
from attrdict import AttrDict as attrdict


print("initialize datadog metrics")
datadog.initialize()
stats = datadog.ThreadStats()
stats.start()

print("open database at pr0gramm-meta.sqlite3")
bottle.install(sqlite.Plugin(dbfile="pr0gramm-meta.sqlite3", dictrows=False))


def metric_name(suffix):
    return "pr0gramm.meta.webapp.%s" % suffix


def query_sizes(database, item_ids):
    where_clause = "id IN (%s)" % ",".join(str(val) for val in item_ids)
    query = "SELECT id, width, height FROM sizes WHERE %s LIMIT 150" % where_clause

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in database.execute(query).fetchall()
    ]

def query_reposts(database, item_ids):
    where_clause = "item_id IN (%s)" % ",".join(str(val) for val in item_ids)
    query = "SELECT item_id FROM tags " \
            " WHERE %s AND +confidence>0.3 AND +tag='repost' COLLATE nocase" \
            " LIMIT 150" % where_clause

    return [item_id for item_id, in database.execute(query).fetchall()]


@bottle.get("/items")
@bottle.post("/items")
def items(db):
    with stats.timer(metric_name("request.items")):
        start_time = time.time()
        item_ids = tuple(int(val) for val in bottle.request.params.get("ids", []).split(",") if val)[:150]

        result = attrdict()
        result.sizes = query_sizes(db, item_ids)
        result.reposts = query_reposts(db, item_ids)
        result.duration = time.time() - start_time
        return result


@bottle.get("/user/<user>")
def user_benis(db, user):
    with stats.timer(metric_name("request.user")):
        query = "SELECT user_score.timestamp, user_score.score" \
                " FROM user_score, users" \
                " WHERE users.name=? COLLATE nocase AND users.id=user_score.user_id AND user_score.timestamp>?"

        start_time = int(time.time() - 3600 * 24 * 7)
        return {"benisHistory": db.execute(query, [user, start_time]).fetchall()}
