import pathlib
import gevent.monkey

gevent.monkey.patch_all()

import subprocess
import re
import sqlite3
import itertools
import time
import gevent
import gevent.queue

from collections import namedtuple
from attrdict import AttrDict as attrdict

from PIL import Image
import logbook
import requests
import datadog
import broke

import json as _json

logger = logbook.Logger("pr0gramm-meta")

logger.info("initialize datadog metrics")
datadog.initialize()
stats = datadog.ThreadStats()
stats.start(flush_in_greenlet=True)



Item = namedtuple("Item", ["id", "promoted", "up", "down",
                           "created", "image", "thumb", "fullsize", "source", "flags",
                           "user", "mark"])

Tag = namedtuple("Tag", ["id", "item_id", "confidence", "tag"])

User = namedtuple("User", ["id", "name", "registered", "score"])

# ensure that the file exists
pathlib.Path("pr0gramm.broke").touch()
broker = broke.BrokeWriter("pr0gramm.broke")

def metric_name(suffix):
    return "pr0gramm.meta.update." + suffix


class UserNameQueue(object):
    def __init__(self):
        self.queue = gevent.queue.Queue()
        self.names = set()

    def put(self, name):
        if name.lower() not in self.names and len(self.names) < 150000:
            self.names.add(name.lower())
            self.queue.put(name)

    def get(self):
        stats.gauge(metric_name("queue.users"), self.queue.qsize())

        name = self.queue.get()
        self.names.discard(name.lower())
        return name

# just put a user in this queue to download its details
user_queue = UserNameQueue()


def iterate_posts(start=None):
    base_url = "http://pr0gramm.com/api/items/get?flags=7"
    while True:
        url = base_url + "&older=%d" % start if start else base_url

        # :type: requests.Response
        with stats.timer(metric_name("request.feed")):
            response = requests.get(url)
            response.raise_for_status()
            json = response.json()

        # dump back to broker
        broker.store("api.items.get", _json.dumps(json).encode("utf8"))

        for item in json["items"]:
            item = Item(**item)
            start = min(start or item.id, item.id)
            yield item

        if json["atEnd"]:
            break


def chunker(n, iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return

        yield chunk


@stats.timed(metric_name("request.user"))
def get_user_details(name):
    url = "http://pr0gramm.com/api/profile/info"
    response = requests.get(url, params={"name": name, "flags": "1"})
    content = response.json()
    user = attrdict(content).user

    # dump back to broker
    broker.store("api.profile.info", _json.dumps(content).encode("utf8"))

    # convert to named tuple
    return User(user.id, user.name, user.registered, user.score)


def store_user_details(db, details):
    with db:
        db.execute("INSERT OR REPLACE INTO users VALUES (?, ?, ?, ?)", details)
        db.execute("INSERT OR REPLACE INTO user_score VALUES (?, ?, ?)",
                   [details.id, int(time.time()), details.score])


def update_user_details(db):
    while True:
        user = user_queue.get()
        try:
            # noinspection PyTypeChecker
            store_user_details(db, get_user_details(user))
            gevent.sleep(1)
        except IOError:
            pass


@stats.timed(metric_name("request.size"), tags=["image"])
def get_image_size(image_url, size=1024):
    # :type: requests.Response
    response = requests.get(image_url, headers={"Range": "bytes=0-%d" % (size - 1)}, stream=True)
    response.raise_for_status()
    try:
        image = Image.open(response.raw)
        return image.size

    finally:
        response.close()


@stats.timed(metric_name("request.size"), tags=["video"])
def get_video_size(video_url, size=16 * 1024):
    # :type: requests.Response
    response = requests.get(video_url, headers={"Range": "bytes=0-%d" % (size - 1)})
    response.raise_for_status()

    # ask avprobe for the size of the image
    process = subprocess.Popen(
        ["ffprobe", "-"], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    stdout, stderr = process.communicate(response.content)

    # and extract result from output
    width, height = re.search(br"Stream.* ([0-9]+)x([0-9]+)", stdout + stderr).groups()
    return int(width), int(height)


def get_item_size(item):
    filename = item.image.lower()
    url = "http://img.pr0gramm.com/" + item.image

    if filename.endswith((".jpg", ".jpeg", ".png", ".gif")):
        for byte_count in [1024, 4096, 8192, 16 * 1024, 64 * 1024]:
            try:
                width, height = get_image_size(url, size=byte_count)
                return width, height
            except IOError:
                pass

    if filename.endswith(".webm"):
        try:
            width, height = get_video_size(url)
            return width, height
        except (OSError, IOError):
            pass

    raise Exception("Could not get size of item {}".format(item.id))


def get_item_ids_in_table(db, items, table):
    ids = ",".join(str(item.id) for item in items)
    query = ";SELECT id FROM %s WHERE id IN (%s)" % (table, ids)
    return {item_id for item_id, in db.execute(query)}


def get_items_not_in_table(db, items, table):
    items_tuple = tuple(items)
    item_ids = get_item_ids_in_table(db, items_tuple, table)
    return [item for item in items_tuple if item.id not in item_ids]


def update_item_sizes(database, items):
    """
    Downloads sizes for a list of items.

    :param sqlite3.Connection database: A database connection to use for storing the items.
    :param tuple[items] items: The items to process
    """
    # get the items that need updates
    for item in get_items_not_in_table(database, items, "sizes"):
        # noinspection PyBroadException
        try:
            width, height = get_item_size(item)

        except KeyboardInterrupt:
            raise

        except:
            logger.exception()
            continue

        with database:
            database.execute("INSERT OR REPLACE INTO sizes VALUES (?, ?, ?)", (item.id, width, height))


def iter_item_tags(item):
    url = "http://pr0gramm.com/api/items/info?itemId=%d" % item.id

    # :type: requests.Response
    response = requests.get(url)
    response.raise_for_status()
    info = response.json()

    # dump back to broker
    broker.store("api.items.info", _json.dumps(info).encode("utf8"))

    # enqueue the commenters names
    for comment in info.get("comments", []):
        user_queue.put(comment["name"])

    for tag in info.get("tags", []):
        yield Tag(tag["id"], item.id, tag["confidence"], tag["tag"])


def update_item_infos(database, items):
    for item in items:
        user_queue.put(item.user)

        # noinspection PyBroadException
        try:
            with stats.timer(metric_name("request.info")):
                tags = tuple(iter_item_tags(item))

        except KeyboardInterrupt:
            raise

        except:
            logger.warn("Could not get tags for item {}", item.id)
            logger.exception()
            continue

        if tags:
            with database:
                stmt = "INSERT OR REPLACE INTO tags (id, item_id, confidence, tag) VALUES (?,?,?,?)"
                database.executemany(stmt, tags)


@stats.timed(metric_name("db.store"))
def store_items(database, items):
    """
    Stores the given items in the database. They will replace any previously stored items.

    :param sqlite3.Connection database: A database connection to use for storing the items.
    :param tuple[items] items: The items to process
    """
    with database:
        stmt = "INSERT OR REPLACE INTO items VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
        database.executemany(stmt, items)

@stats.timed(metric_name("broker.commit"))
def broker_commit():
    if broker.dirty:
        logger.info("Committing broker to filesystem now")
        broker.commit()
    else:
        logger.info("Broker is empty, nothing to commit")


def create_database_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS items (
      id INT PRIMARY KEY,
      promoted INT, up INT, down INT, created INT,
      image TEXT, thumb TEXT, fullsize TEXT, source TEXT, flags INT, user TEXT, mark INT
    )""")

    db.execute("CREATE TABLE IF NOT EXISTS sizes (id INT PRIMARY KEY, width INT, height INT)")
    db.execute("""CREATE TABLE IF NOT EXISTS tags (
      id INT PRIMARY KEY,
      item_id INT,
      confidence REAL,
      tag TEXT,
      FOREIGN KEY (item_id) REFERENCES items(id)
    )""")

    db.execute("""CREATE TABLE IF NOT EXISTS users (
      id INT PRIMARY KEY,
      name TEXT,
      registered INT,
      score INT)
    """)

    db.execute("""CREATE TABLE IF NOT EXISTS user_score (
      user_id INT,
      timestamp INT,
      score INT,
      FOREIGN KEY (user_id) REFERENCES users(id)
    )""")

    db.execute("CREATE INDEX IF NOT EXISTS tags_item_id ON tags(item_id)")
    db.execute("CREATE INDEX IF NOT EXISTS users_name ON users(name COLLATE NOCASE)")
    db.execute("CREATE INDEX IF NOT EXISTS user_score__user_id__timestamp ON user_score(user_id, timestamp)")


def schedule(interval, name, func, *args, **kwargs):
    def worker():
        while True:
            start = time.time()

            # noinspection PyBroadException
            try:
                logger.info("Calling scheduled function {} now", name)
                func(*args, **kwargs)

                duration = time.time() - start
                logger.info("{} took {:1.2f}s to complete", name, duration)

            except KeyboardInterrupt:
                raise

            except:
                duration = time.time() - start
                logger.exception("Ignoring error in scheduled function {} after {}", name, duration)

            gevent.sleep(interval)

    return gevent.spawn(worker)


def run(db, *functions):
    for items in chunker(16, iterate_posts()):
        stop = True
        age = (time.time() - items[0].created) / 3600
        for min_age, max_age, function in functions:
            if age < min_age:
                stop = False
                continue

            if age > max_age:
                continue

            store_items(db, items)
            function(db, items)
            stop = False

        if stop:
            break


def main():
    logger.info("opening database")
    db = sqlite3.connect("pr0gramm-meta.sqlite3")
    create_database_tables(db)

    def start():
        yield schedule(1, "pr0gramm.meta.update.users", update_user_details, db)

        yield schedule(60, "pr0gramm.meta.update.sizes",
                       run, db, (0, 0.5, update_item_sizes), (0, 0.5, update_item_infos))

        yield schedule(600, "pr0gramm.meta.update.infos.new",
                       run, db, (0, 6, update_item_infos))

        yield schedule(3600, "pr0gramm.meta.update.infos.more",
                       run, db, (5, 48, update_item_infos))

        yield schedule(24 * 3600, "pr0gramm.meta.update.infos.day",
                       run, db, (47, 24 * 7, update_item_infos))

        gevent.sleep(15)
        yield schedule(300, "app.broke.commit", broker_commit)

    try:
        gevent.joinall(tuple(start()))
    except KeyboardInterrupt:
        broker_commit()


if __name__ == '__main__':
    file_handler = logbook.FileHandler("logfile.log", bubble=True)
    with file_handler.applicationbound():
        main()
