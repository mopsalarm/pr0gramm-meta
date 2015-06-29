import sqlite3
import argparse

import psycopg2


def copy(table, source, cursor):
    for idx, row in enumerate(source.execute("SELECT * FROM %s" % table)):
        placeholders = ",".join(["%s"] * len(row))
        cursor.execute("insert into %s VALUES(%s)" % (table, placeholders), row)

        if idx % 100000 == 0:
            print table, idx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--source", default="pr0gramm-meta.sqlite3")
    args = parser.parse_args()

    source = sqlite3.connect("pr0gramm-meta.sqlite3")
    target = psycopg2.connect(host=args.pg_host, user="postgres", password="password", dbname="postgres")

    target_cursor = target.cursor()
    copy("items", source, target_cursor)
    copy("sizes", source, target_cursor)
    copy("tags", source, target_cursor)

    target.commit()


if __name__ == '__main__':
    main()
