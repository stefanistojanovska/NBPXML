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

    post_comments = {}

    with io.open(r"comments.csv", 'r', encoding="utf8", newline='') as comments:
        reader_comments = csv.reader(comments, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)

        for comment in reader_comments:
            current_comment = {
                "content": comment[2],
                "author": comment[3],
                "date": comment[4],
                "vote": comment[5]
            }
            if "post_" + comment[1] in post_comments:
                post_comments["post_" + comment[1]]["comment_" + comment[0]] = current_comment
            else:
                post_comments["post_" + comment[1]] = {}
                post_comments["post_" + comment[1]]["comment_" + comment[0]] = current_comment

    post_inlinks = {}
    with io.open(r"inlinks.csv", 'r', encoding="utf8", newline='') as inlinks:
        reader_inlinks = csv.reader(inlinks, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)

        for inlink in reader_inlinks:
            current_inlink = {
                "title": inlink[2],
                "author": inlink[3],
                "date": inlink[4],
                "url": inlink[5]
            }
            if "post_" + inlink[1] in post_inlinks:
                post_inlinks["post_" + inlink[1]]["inlink_" + inlink[0]] = current_inlink
            else:
                post_inlinks["post_" + inlink[1]] = {}
                post_inlinks["post_" + inlink[1]]["inlink_" + inlink[0]] = current_inlink

    authors_posts = {}
    with io.open(r"posts.csv", 'r', encoding="utf8", newline='') as posts:
        reader_posts = csv.reader(posts, delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)

        for post in reader_posts:
            current_post = {
                "title": post[1],
                "comments_number": post[4],
                "content": post[5],
                "url": post[6],
                "date": post[7],
                "retrieved_links_number": post[8],
                "retrieved_comments_number": post[9],
                "comments": []


            }
            if 'post_'+post[0] in post_comments:
                current_post['comments'] = post_comments['post_'+post[0]]
            if 'post_' + post[0] in post_inlinks:
                current_post['inlinks'] = post_inlinks['post_' + post[0]]

            if "author_" + post[3] in authors_posts:
                authors_posts["author_" + post[3]]["post_" + post[0]] = current_post
            else:
                authors_posts["author_" + post[3]] = {}
                authors_posts["author_" + post[3]]["post_" + post[0]] = current_post
    #print(authors_posts['author_1']['post_1'])
    start = time.time()
    for key in authors_posts:
        k = ('test', 'nbp' , key)
        bin = {
            "posts": authors_posts[key]
        }
	
        client.put(k, bin)
    #for post in post_inlinks:
    time_elapsed = time.time() - start
    print("--- VKUPNO: %s ---" % time_elapsed)
