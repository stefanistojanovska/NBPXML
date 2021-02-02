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
    with io.open(r"inlinks.csv", 'r', encoding="utf8", newline='') as inlinks:
        reader_inlinks = csv.reader(inlinks, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start = time.time()
        for inlink in reader_inlinks:
            current_inlink = {
                "title": inlink[2],
                "author": inlink[3],
                "date": inlink[4],
                "url": inlink[5]
            }
            #start = time.time()
            key = ('test', 'nbp2', 'post_'+inlink[1])
            client.map_put(key, "inlinks", "inlink_"+inlink[0], current_inlink)
            #print(time.time() - start)
        time_elapsed = time.time() - start
        print("---VKUPNO: %s  ---" % time_elapsed)
