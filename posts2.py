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

    with io.open(r"posts.csv", 'r', encoding="utf8", newline='') as posts:
        reader_posts = csv.reader(posts, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start = time.time()
        for post in reader_posts:
            current_post = {
                "title": post[1],
                "comments_number": post[4],
                "content": post[5],
                "url": post[6],
                "date": post[7],
                "ret_links_num": post[8],
                "ret_comms_num": post[9],
            }
            #start = time.time()
            key = ('test', 'nbp2', 'post_'+post[0])
            client.put(key, current_post)
        time_elapsed = time.time() - start
        print("---VKUPNO: %s  ---" % time_elapsed)
     
