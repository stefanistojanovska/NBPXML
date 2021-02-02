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
       start_time = time.time()
       for author in reader_authors:
            key = ('test', 'nbp', 'author_'+author[0])
            #print(client.remove(key))
            #print(author[0])
    #client = aerospike.client(config)
    #client.connect()
    key = ('test', 'nbp', 'author_1')
    bin = {"lala" : "swd"}
    #client.put(key, bin)
    #(key, meta, bins) = client.select(('test','nbp','author_1'), ('name','posts'[2]))
    #(key, meta, record) = client.get(key).get('posts')
    #client.get(key).get('posts')
    print(bins)
    #print(client.scan('test', 'nbp'))
