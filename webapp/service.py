import time
import sqlite3

import bottle
import datadog
from attrdict import AttrDict as attrdict


print("initialize datadog metrics")
datadog.initialize()
stats = datadog.ThreadStats()
stats.start()

print("open database at pr0gramm-meta.sqlite3")
database = sqlite3.connect("pr0gramm-meta.sqlite3", timeout=1)


def metric_name(suffix):
    return "pr0gramm.meta.webapp.%s" % suffix


def query_sizes(where_clause):
    query = "SELECT items.id, width, height FROM items" \
            " JOIN sizes ON items.id=sizes.id " \
            " WHERE %s" \
            " LIMIT 150" % where_clause

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in database.execute(query).fetchall()
    ]


def query_reposts(where_clause):
    query = "SELECT DISTINCT items.id FROM items " \
            " JOIN tags ON items.id=tags.item_id " \
            " WHERE %s AND tags.confidence>0.3 AND tags.tag='repost' COLLATE NOCASE" \
            " LIMIT 150" % where_clause

    return [item_id for item_id, in database.execute(query).fetchall()]


@bottle.get("/items")
@bottle.post("/items")
@stats.timed(metric_name("request.items"))
def items():
    start_time = time.time()
    item_ids = [int(val) for val in bottle.request.params.get("ids", []).split(",") if val][:150]

    where_clause = "items.id IN (%s)" % ",".join(str(val) for val in item_ids)

    result = attrdict()
    result.sizes = query_sizes(where_clause)
    result.reposts = query_reposts(where_clause)
    result.duration = time.time() - start_time
    return result


@bottle.get("/user/<user>")
@stats.timed(metric_name("request.user"))
def user_benis(user):
    query = "SELECT user_score.timestamp, user_score.score" \
            " FROM user_score, users" \
            " WHERE users.name=? COLLATE NOCASE AND users.id=user_score.user_id"

    return {"benisHistory": database.execute(query, [user]).fetchall()}
