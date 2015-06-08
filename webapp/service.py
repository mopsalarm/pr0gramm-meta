import gevent.monkey

gevent.monkey.patch_all()

import sqlite3
import time

import bottle

import datadog

print "initialize datadog metrics"
datadog.initialize()
stats = datadog.ThreadStats()
stats.start(flush_in_greenlet=True)

print "open database at pr0gramm-meta.sqlite3"
database = sqlite3.connect("pr0gramm-meta.sqlite3")

def metric_name(suffix):
    return "pr0gramm.meta.webapp.%s" % suffix

def get_sizes(where_clause):
    query = "SELECT items.id, width, height FROM items" \
            " JOIN sizes ON items.id=sizes.id " \
            " WHERE %s" \
            " LIMIT 150" % where_clause

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in database.execute(query).fetchall()
    ]


def get_reposts(where_clause):
    query = "SELECT DISTINCT items.id FROM items " \
            " JOIN tags ON items.id=tags.item_id " \
            " WHERE %s AND tags.confidence>0.05 AND tags.tag='repost' COLLATE NOCASE" \
            " LIMIT 150" % where_clause

    return [item_id for item_id, in database.execute(query).fetchall()]


@stats.timed(metric_name("lookup"))
def lookup_items(where_clause):
    stats.increment(metric_name("request"))
    start_time = time.time()

    result = dict(
        sizes=get_sizes(where_clause),
        reposts=get_reposts(where_clause)
    )

    result["duration"] = time.time() - start_time
    return result

@bottle.get("/items")
def items():
    item_ids = [int(val) for val in bottle.request.params.get("ids", []).split(",") if val]
    item_ids = item_ids[:150]

    return lookup_items("items.id IN (%s)" % ",".join(str(val) for val in item_ids))
