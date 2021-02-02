from __future__ import print_function

import io
import csv
import aerospike
import json
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
            start = time.time()
            key = ('test','nbp','author_' + author[0])
            #client.remove(key)
            #print(time.time() - start)
            #print(author[0])
    #client = aerospike.client(config)
    #client.connect()
    start = time.time()
    key = ('test', 'nbp2', 'post_19333')
    #bin = {"lala" : "swd"}
    #client.put(key, bin)
    #(key, meta, bins) = client.select(('test','nbp','author_1'), ('name','posts'[0]))
    (key, meta, record) = client.get(key)
    #query = client.query( 'test', 'nbp') 
    print(time.time() - start)
    #print(client.map_get_by_index_range(key, 'comments', 0, 1, aerospike.MAP_RETURN_VALUE))
    #response = client.info("sets")
    #query.where(aerospike.predicates.between('meibix', 2, 4) )
    #client.get(key).get('posts')
    print(record)
    #query.foreach(print_result)
    #print(client.scan('test', 'nbp'))
