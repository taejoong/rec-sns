#-*- coding: utf-8 -*- 
import MySQLdb
conn = MySQLdb.connect('localhost', 'root', 'mmlab2013', 'sns', charset='utf8', use_unicode=True)
cursor = conn.cursor()
def twitterTagSpliter(path, output_path):
    w = open(output_path, "w")
    r = open(path, "r")
    lines = r.read()
    for line in lines.split("]}"):
        cid, time, text, words = line.split("\t")
        tags = map(lambda v: "#" + v.split(" ")[0], text.split("#")[1:])

        #w.write("%s\t%s\t%s\t%s\t%s" % (cid, time, text.encode('utf-8'), words + "]}", " ".join(tags)))

        query = """ INSERT INTO twitter_150330_meta_word (content_id, time, text, words, text_tags) 
                    VALUES (%s, %s, %s, %s, %s) 
                    """
        #print query % (cid, time, text, words + "]}", tags)
        cursor.execute(query, (cid, time, text, words + "]}", " ".join(tags)))
        conn.commit()

    #w.close()

"""
# Regard keyword as a hashtag
def youtubeTagMaker(path, output_path):
    w = open(output_path, "w")
    for line in open(path):
        cid, keyword, type, id, etag, publishedAt, channelID, title, desc,channelTitle, playlistId, videoId, words = line.split("\t")
"""




        


if __name__ == "__main__":
    twitterTagSpliter("../twitter_150330_meta.tsv", "../twitter_150330_meta_hashtag.tsv")
