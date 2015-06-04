import sqlite3
import time

import bottle
import datadog


datadog.initialize()

database = sqlite3.connect("pr0gramm-meta.sqlite3")


def get_sizes(lower, upper, promoted):
    query = "SELECT items.id, width, height FROM items" \
            " JOIN sizes ON items.id==sizes.id " \
            " WHERE items.id>=? AND items.id<=? %s" \
            " LIMIT 150" % ("AND items.promoted!=0" if promoted else "")

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in database.execute(query, [lower, upper]).fetchall()
    ]


def get_reposts(lower, upper, promoted):
    query = "SELECT DISTINCT items.id FROM items " \
            " JOIN tags ON items.id==tags.item_id " \
            " WHERE items.id>=? AND items.id<=?" \
            "       AND tags.confidence>0.3 AND tags.tag=='repost' COLLATE NOCASE %s" \
            " LIMIT 150" % ("AND items.promoted!=0" if promoted else "")

    return [item_id for item_id, in database.execute(query, [lower, upper]).fetchall()]


@datadog.statsd.timed("pr0gramm.meta.webapp.lookup")
def lookup_items_between(first_id, second_id, promoted):
    start_time = time.time()

    lower = max(1, min(first_id, second_id))
    upper = max(first_id, second_id)

    result = dict(
        sizes=get_sizes(lower, upper, promoted),
        repost=get_reposts(lower, upper, promoted)
    )

    result["duration"] = time.time() - start_time
    return result

for urlstr, promoted in dict(new=False, top=True).items():
    # noinspection PyShadowingNames
    def generate(promoted):
        @bottle.get("/items/%s/between/<first_id:int>/<second_id:int>" % urlstr)
        def items_between(first_id, second_id):
            return lookup_items_between(first_id, second_id, promoted)

        @bottle.get("/items/%s/before/<first_id:int>" % urlstr)
        def items_before(first_id):
            return lookup_items_between(first_id, first_id - 500, promoted)

        @bottle.get("/items/%s/after/<first_id:int>" % urlstr)
        def items_before(first_id):
            return lookup_items_between(first_id, first_id + 500, promoted)

    generate(promoted)
