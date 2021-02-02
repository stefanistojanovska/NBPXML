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

    with io.open(r"authors.csv", 'r', encoding="utf8", newline='') as authors:
        reader_authors = csv.reader(authors, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start = time.time()
        for author in reader_authors:
            current_author = {
                "name": author[1],
                "meibi": author[2],
                "meibix": author[3],
                "avg_words": author[4],
                "avg_w_no_sw": author[5],
            }
            key = ('test', 'nbp', 'author_'+author[0])
            client.put(key, current_author)
            
            #(key, metadata, record) = client.get(key)
            #print(record)
            
        time_elapsed = time.time() - start
        print("--- VKUPNO: %s ---" % time_elapsed)
