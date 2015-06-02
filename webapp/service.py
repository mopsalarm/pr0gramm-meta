import sqlite3
import time

import bottle


database = sqlite3.connect("pr0gramm-meta.sqlite3")


def get_sizes(lower, upper):
    query = "SELECT items.id, width, height FROM items, sizes" \
            " WHERE items.id==sizes.id AND items.id>=? AND items.id<=?"

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in database.execute(query, [lower, upper]).fetchall()
    ]


def get_reposts(lower, upper):
    query = "SELECT DISTINCT item_id FROM tags" \
            " WHERE item_id>=? AND item_id<=? AND tag=='repost' COLLATE NOCASE"

    return [item_id for item_id, in database.execute(query, [lower, upper]).fetchall()]


@bottle.get("/items/<first_id:int>/<second_id:int>")
def items(first_id, second_id):
    start_time = time.time()

    lower = min(first_id, second_id)
    upper = min(lower + 150, max(first_id, second_id))

    result = dict(
        sizes=get_sizes(lower, upper),
        repost=get_reposts(lower, upper)
    )

    result["duration"] = time.time() - start_time
    return result
