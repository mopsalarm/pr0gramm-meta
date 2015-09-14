import argparse
import sqlite3

import psycopg2


def chunks(values, size=1000):
    result = []
    for value in values:
        result.append(value)
        if len(result) == size:
            yield result
            result = []

    if result:
        yield result


def copy(table, source, cursor):
    for idx, chunk in enumerate(chunks(source.execute("SELECT * FROM %s" % table))):
        values = b",".join(cursor.mogrify("(" + ",".join(["%s"] * len(row)) + ")", row) for row in chunk)
        cursor.execute(b"insert into " + table.encode("utf8") + b" VALUES " + values)

        if idx % 100 == 0:
            print(table, 1000 * idx)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--source", default="pr0gramm-meta.sqlite3")
    args = parser.parse_args()

    source = sqlite3.connect("pr0gramm-meta.sqlite3")
    source.text_factory = str
    target = psycopg2.connect(host=args.pg_host, user="postgres", password="password", dbname="postgres")

    target_cursor = target.cursor()
    copy("items", source, target_cursor)
    copy("sizes", source, target_cursor)
    copy("tags", source, target_cursor)
    copy("users", source, target_cursor)
    copy("user_score", source, target_cursor)
    copy("controversial", source, target_cursor)

    target.commit()


if __name__ == '__main__':
    main()
