#!/usr/bin/env python3

import argparse
import itertools
import queue
import re
import subprocess
import sys
import threading
import time
from collections import deque
from collections import namedtuple
from io import BytesIO

import datadog
import logbook
import pcc
import requests
from PIL import Image
from attrdict import AttrDict as attrdict

# noinspection PyUnresolvedReferences
import signal

logger = logbook.Logger("pr0gramm-meta")

logger.info("initialize datadog metrics")
datadog.initialize()
stats = datadog.ThreadStats()
stats.start()

Item = namedtuple("Item", ["id", "promoted", "up", "down",
                           "created", "image", "thumb", "fullsize", "source", "flags",
                           "user", "mark"])

Tag = namedtuple("Tag", ["id", "item_id", "confidence", "tag"])

User = namedtuple("User", ["id", "name", "registered", "score"])


def metric_name(suffix):
    return "pr0gramm.meta.update." + suffix


class SetQueue(queue.Queue):
    """This queue only contains unique values"""

    def __init__(self, maxsize=0, key=lambda x: x):
        super().__init__(maxsize)
        self.keyfunc = key

    # Initialize the queue representation
    def _init(self, maxsize):
        self.keys = set()
        self.queue = deque()

    def _qsize(self):
        assert len(self.queue) == len(self.keys), "length of queue and keys not equal"
        return len(self.queue)

    # Put a new item in the queue
    def _put(self, item):
        key = self.keyfunc(item)
        if key not in self.keys:
            self.keys.add(key)
            self.queue.append(item)

    # Get an item from the queue
    def _get(self):
        item = self.queue.popleft()
        self.keys.remove(self.keyfunc(item))
        return item


class UserSetQueue(SetQueue):
    def __init__(self):
        super().__init__(key=str.lower)

    def _put(self, item):
        stats.gauge(metric_name("queue.users"), len(self.keys), sample_rate=0.01)
        super()._put(item)

    def _get(self):
        stats.gauge(metric_name("queue.users"), len(self.keys), sample_rate=0.01)
        return super()._get()


# just put a user in this queue to download its details
user_queue = UserSetQueue()


def iterate_posts(start=None):
    base_url = "http://pr0gramm.com/api/items/get?flags=7"
    while True:
        url = base_url + "&older=%d" % start if start else base_url

        # :type: requests.Response
        with stats.timer(metric_name("request.feed")):
            response = requests.get(url)
            response.raise_for_status()
            json = response.json()

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

    # convert to named tuple
    return User(user.id, user.name, user.registered, user.score)


def store_user_details(database, details):
    with database, database.cursor() as cursor:
        cursor.execute("INSERT INTO users VALUES (%s, %s, %s, %s)"
                       " ON CONFLICT(id) DO UPDATE SET score=%s",
                       list(details) + [details.score])

        cursor.execute("INSERT INTO user_score VALUES (%s, %s, %s)",
                       [details.id, int(time.time()), details.score])


def update_user_details(dbpool):
    while True:
        user = user_queue.get()
        try:
            # noinspection PyTypeChecker
            with dbpool.active() as database:
                store_user_details(database, get_user_details(user))

            time.sleep(1)
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
            ["timeout", "ffprobe", "-"], shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

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
    query = "SELECT id FROM %s WHERE id IN (%s)" % (table, ids)

    with db, db.cursor() as cursor:
        cursor.execute(query)
        return {item_id for item_id, in cursor}


def get_items_not_in_table(db, items, table):
    items_tuple = tuple(items)
    item_ids = get_item_ids_in_table(db, items_tuple, table)
    return [item for item in items_tuple if item.id not in item_ids]


def update_item_sizes(database, items):
    """
    Downloads sizes for a list of items.

    :param database: A database connection to use for storing the items.
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

        with database, database.cursor() as cursor:
            cursor.execute("INSERT INTO sizes VALUES (%s, %s, %s)"
                           " ON CONFLICT(id) DO NOTHING", (item.id, width, height))


def update_item_previews(database, items):
    # get the items that need updates
    for item in get_items_not_in_table(database, items, "item_previews"):
        # noinspection PyBroadException
        try:
            filename = item.image.lower()
            url = "http://img.pr0gramm.com/" + item.image
            logger.debug("Update preview for {}", url)

            # generate thumbnail
            png_bytes = subprocess.check_output([
                "timeout",
                "ffmpeg", "-loglevel", "panic", "-y", "-i", url,
                "-vf", "scale=8:-1", "-frames", "1",
                "-f", "image2", "-vcodec", "png", "-"])

            image = Image.open(BytesIO(png_bytes)).convert("RGB")
            width, height = image.size

            preview = bytearray()
            for r, g, b in image.getdata():
                # rrrrrggg gggbbbbb
                first = (r & 0xf8) | (g >> 5)
                second = ((g >> 2) & 0x7) | (b >> 3)
                preview.append(first)
                preview.append(second)

            with database, database.cursor() as cursor:
                cursor.execute("INSERT INTO item_previews VALUES (%s, %s, %s, %s) ON CONFLICT(id) DO NOTHING",
                               (item.id, width, height, preview))

        except KeyboardInterrupt:
            raise

        except:
            logger.exception()
            continue


def iter_item_tags(item):
    url = "http://pr0gramm.com/api/items/info?itemId=%d" % item.id

    # :type: requests.Response
    response = requests.get(url)
    response.raise_for_status()
    info = response.json()

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
            tags = [list(tag) + [tag.confidence] for tag in tags]
            stmt = "INSERT INTO tags (id, item_id, confidence, tag) VALUES (%s,%s,%s,%s)" \
                   " ON CONFLICT(id) DO UPDATE SET confidence=%s"

            with database, database.cursor() as cursor:
                cursor.executemany(stmt, tags)


@stats.timed(metric_name("db.store"))
def store_items(database, items):
    """
    Stores the given items in the database. They will replace any previously stored items.

    :param tuple[items] items: The items to process
    """
    items = [list(item) + [item.up, item.down, item.mark] for item in items]
    stmt = "INSERT INTO items VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" \
           " ON CONFLICT(id) DO UPDATE SET up=%s, down=%s, mark=%s"

    with database, database.cursor() as cursor:
        cursor.executemany(stmt, items)


def schedule(interval, name, func, *args, **kwargs):
    while True:
        start = time.time()

        # noinspection PyBroadException
        try:
            logger.info("Calling scheduled function {} now", name)
            func(*args, **kwargs)

            duration = time.time() - start
            logger.info("{} took {:1.2f}s to complete", name, duration)

        except KeyboardInterrupt:
            sys.exit(1)

        except:
            duration = time.time() - start
            logger.exception("Ignoring error in scheduled function {} after {}", name, duration)

        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            sys.exit(1)


def run(dbpool, *functions):
    for items in chunker(16, iterate_posts()):
        stop = True
        age = (time.time() - items[0].created) / 3600
        for min_age, max_age, function in functions:
            if age < min_age:
                stop = False
                continue

            if age > max_age:
                continue

            with dbpool.active() as database:
                store_items(database, items)
                function(database, items)

            stop = False

        if stop:
            break


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--postgres", type=str, required=True, help="Postgres host")
    return parser.parse_args()


def start(dbpool):
    def start_in_thread(func, *args):
        thread = threading.Thread(target=func, args=args, daemon=True)
        thread.start()
        return thread

    yield start_in_thread(schedule, 1, "pr0gramm.meta.update.users", update_user_details, dbpool)

    yield start_in_thread(schedule, 60, "pr0gramm.meta.update.sizes",
                          run, dbpool,
                          (0, 0.5, update_item_previews),
                          (0, 0.5, update_item_sizes),
                          (0, 0.5, update_item_infos))

    yield start_in_thread(schedule, 600, "pr0gramm.meta.update.infos.new",
                          run, dbpool, (0, 6, update_item_infos))

    yield start_in_thread(schedule, 3600, "pr0gramm.meta.update.infos.more",
                          run, dbpool, (5, 48, update_item_infos))

    yield start_in_thread(schedule, 24 * 3600, "pr0gramm.meta.update.infos.week",
                          run, dbpool, (47, 24 * 7, update_item_infos))


def main():
    args = parse_arguments()

    logger.info("opening database at {}", args.postgres)
    pool = pcc.RefreshingConnectionCache(
            lifetime=600,
            host=args.postgres, user="postgres", password="password", dbname="postgres")

    try:
        threads = tuple(start(pool))
        for thread in threads:
            thread.join()

    except KeyboardInterrupt:
        sys.exit(1)


if __name__ == '__main__':
    file_handler = logbook.FileHandler("logfile.log", bubble=True)
    with file_handler.applicationbound():
        main()
