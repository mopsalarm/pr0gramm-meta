import os
import time

import bottle
import datadog
from first import first
from attrdict import AttrDict as attrdict
from pcc import RefreshingConnectionCache

CONFIG_POSTGRES_HOST = os.environ["POSTGRES_HOST"]

print("initialize datadog metrics")
datadog.initialize()
stats = datadog.ThreadStats()
stats.start()

print("open database at", CONFIG_POSTGRES_HOST)

pool = RefreshingConnectionCache(
        lifetime=600,
        host=CONFIG_POSTGRES_HOST, user="postgres", password="password", dbname="postgres")


def metric_name(suffix):
    return "pr0gramm.meta.webapp.%s" % suffix


def execute(query, *args):
    with pool.tx() as database, database.cursor():
        cursor = database.cursor()
        cursor.execute(query, *args)
        return cursor.fetchall()


def query_sizes(item_ids):
    where_clause = "id IN (%s)" % ",".join(str(val) for val in item_ids)
    query = "SELECT id, width, height FROM sizes WHERE %s LIMIT 150" % where_clause

    return [
        dict(id=item_id, width=width, height=height)
        for item_id, width, height in execute(query)
        ]


def query_reposts(item_ids):
    where_clause = "item_id IN (%s)" % ",".join(str(val) for val in item_ids)
    query = "SELECT item_id FROM tags " \
            " WHERE %s AND +confidence>0.3 AND lower(tag)='repost'" \
            " LIMIT 150" % where_clause

    return [item_id for item_id, in execute(query)]


@bottle.get("/items")
@bottle.post("/items")
def items():
    with stats.timer(metric_name("request.items")):
        start_time = time.time()
        item_ids = tuple(int(val) for val in bottle.request.params.get("ids", []).split(",") if val)[:150]

        result = attrdict()
        result.sizes = query_sizes(item_ids)
        result.reposts = query_reposts(item_ids)
        result.duration = time.time() - start_time
        return result


@bottle.get("/user/<user>")
def user_benis(user):
    with stats.timer(metric_name("request.user")):
        query = "SELECT user_score.timestamp, user_score.score" \
                " FROM user_score, users" \
                " WHERE lower(users.name)=lower(%s) AND users.id=user_score.user_id AND user_score.timestamp>%s"

        start_time = int(time.time() - 3600 * 24 * 7)
        return {"benisHistory": execute(query, [user, start_time])}


@bottle.get("/user/suggest/<prefix>")
def user_suggest(prefix):
    prefix = prefix.strip().replace("%", "")
    if len(prefix) < 3:
        return bottle.abort(412, "Need at least 3 chars")

    with stats.timer(metric_name("request.user-suggest")):
        query = "SELECT name FROM users WHERE lower(name) LIKE lower(%s) ORDER BY score DESC LIMIT 20"
        return {"names": [first(row) for row in execute(query, [prefix.replace("%", "") + "%"])]}
