from __future__ import print_function

import io
import csv
import aerospike
import time

if __name__ == '__main__':
    config = {
        'hosts': [
            ('127.0.0.1', 3000)
        ],
        'policies': {
            'timeout': 1000  # milliseconds
        }
    }
    client = aerospike.client(config)
    client.connect()
    with io.open(r"comments.csv", 'r', encoding="utf8", newline='') as comments:
        reader_comments = csv.reader(comments, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start = time.time()
        for comment in reader_comments:
            current_comment = {
                "content": comment[2],
                "author": comment[3],
                "date": comment[4],
                "vote": comment[5]
            }
            #start = time.time()
            key = ('test', 'nbp2', 'post_'+comment[1])
            client.map_put(key, "comments", "comment_"+comment[0], current_comment)
        time_elapsed = time.time() - start
        print(" --- VKUPNO: %s ---" % time_elapsed)
