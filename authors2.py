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
    post_author = {}
    with io.open(r"posts.csv", 'r', encoding="utf8", newline='') as posts:
        reader_posts = csv.reader(posts, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        for post in reader_posts:
            author_id = post[3]
            post_id = post[0]
            if author_id in post_author.keys():
                post_author[author_id].append(post_id)
            else:
                post_author[author_id] = []
                post_author[author_id].append(post_id)

    with io.open(r"authors.csv", 'r', encoding="utf8", newline='') as authors:
        reader_authors = csv.reader(authors, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
        start = time.time()
        for author in reader_authors:
            current_author = {
                "name": author[1],
                "meibi": author[2],
                "meibix": author[3],
                "avg_words": author[4],
                "avg_words_no_stopwords": author[5]
            }
            for post_id in post_author[author[0]]:
                key = ('test', 'nbp2', 'post_'+post_id)
                client.put(key, {'author' : current_author})
        time_elapsed = time.time() - start
        print("--- VKUPNO: %s ---" % time_elapsed)
