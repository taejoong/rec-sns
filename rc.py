#-*- coding: utf-8 -*- 
import sys
import json
import MySQLdb
from igraph import *
conn = MySQLdb.connect('localhost', 'root', 'mmlab2013', 'sns', charset='utf8', use_unicode=True)
cursor = conn.cursor()

MAX_SEARCH = 100
WEIGHT_THRESHOLD = 5
WEIGHT_DIFFERENT_MEDIA = 2
WEIGHT_HASH_TAG = 1.5
WEIGHT_MORPHEME = {'NNP': 0.5, 'OL': 0.3, 'NNG': 0.1}
        
def getContent(cid):
    if(cid[0] == "T"):
        selecter = "text"
        table = "twitter_150330_meta_word"

    elif(cid[0] == "I"):
        selecter = "text_desc"
        table = "instagram_150330_meta_word"

    else:
        selecter = "title"
        table = "youtube_150330_meta_word"

    query = """
        SELECT %s
        FROM %s
        WHERE content_id = %s
        """ % (selecter, table, cid[2:])

    conn.query(query)
    r = conn.store_result()
    return r.fetch_row(1)[0][0]

def getRelatedKeyword(keyword):
    query = """
    """

def convertWhereStatement(condition, keywords):
    where = []
    for keyword in keywords:
        where.append( "(" + condition + ")")

    where_sentence = " and ".join(where)
    return where_sentence % tuple(keywords)

def getCntCommonWords(list_a, list_b):
    return len(set(list_a).intersection(set(list_b)))

def searchKeyword(keywords):
    #1. Twitter query
    where = convertWhereStatement("""text_tags LIKE "%%%s%%" """, keywords)
    query = """
        SELECT CONCAT("T:", content_id), text_tags, words 
        FROM twitter_150330_meta_word
        WHERE """ + where

    conn.query(query)
    r = conn.store_result()
    
    twitter_dict = {}
    for content_id, text_tags, words in r.fetch_row(MAX_SEARCH):
        twitter_dict[content_id] = (text_tags, words)
    
    print query 

    #2. Instagram query
    where = convertWhereStatement("""text_tags LIKE "%%%s%%" """, keywords)
    query = """
        SELECT CONCAT("I:", content_id), text_tags, words 
        FROM instagram_150330_meta_word
        WHERE """  + where
    
    conn.query(query)
    r = conn.store_result()
    
    instagram_dict = {}
    for content_id, text_tags, words in r.fetch_row(MAX_SEARCH):
        instagram_dict[content_id] = (text_tags, words)

    #3. Youtube query
    where1 = convertWhereStatement("""title LIKE "%%%s%%" """, keywords)
    where2 = convertWhereStatement("""description LIKE "%%%s%%" """, keywords)

    query = """
        SELECT CONCAT("Y:", content_id), title, words 
        FROM youtube_150330_meta_word
        WHERE """ + where1 + " or " + where2

    conn.query(query)
    r = conn.store_result()
    
    youtube_dict = {}
    for content_id, text_tags, words in r.fetch_row(MAX_SEARCH):
        youtube_dict[content_id] = (text_tags, words)
    
    return twitter_dict, instagram_dict, youtube_dict

def getRelation(c1_name, (c1_hashtag, c1_morpheme), 
                 c2_name, (c2_hashtag, c2_morpheme)):
    c1_media = c1_name[0]
    c2_media = c2_name[0]
    
    #print c1_name, c1_hashtag.encode('utf-8'), c1_morpheme
    #print c2_name, c2_hashtag.encode('utf-8'), c2_morpheme

    weight = 0

    ## heterogeneous media?
    if(c1_media != c2_media):
        weight += WEIGHT_DIFFERENT_MEDIA

    ## multiple matched hashtag?
    c1_hashtag = c1_hashtag.split("#")
    c2_hashtag = c2_hashtag.split("#")

    weight += getCntCommonWords(c1_hashtag, c2_hashtag) * WEIGHT_HASH_TAG
    #print weight 

    ## multiple matched morpheme?
    ## NNG: common noun
    ## OL: foreign language 
    ## NNP: proper noun, 
    
    try: 
        c1_morpheme = json.loads(c1_morpheme)
        c2_morpheme = json.loads(c2_morpheme)
    
        for morpheme in ['NNP', 'OL', 'NNG']:
            weight += getCntCommonWords(c1_morpheme[morpheme], c2_morpheme[morpheme]) * WEIGHT_MORPHEME[morpheme]
            #print morpheme, weight
    except:
        pass
    
    return weight

def makeGraph(t_dict, i_dict, y_dict):
    combine_dict = t_dict.copy()
    combine_dict.update(i_dict)
    combine_dict.update(y_dict)

    combine_list = combine_dict.keys()
    edge_list = []
    for c1 in combine_list:
        print "%s / %s" % (combine_list.index(c1), len(combine_list))
        for c2 in combine_list[combine_list.index(c1) + 1:]:
            weight = getRelation(c1, combine_dict[c1], c2, combine_dict[c2])
            if(weight >= WEIGHT_THRESHOLD):
                edge_list.append((c1, c2, weight))
    return edge_list

def makeCommunity(edge_list):
    g = Graph()
    for v1, v2, weight in edge_list:
        if('name' in g.vs):
            if(v1 not in g.vs['name']):
                g.add_vertex(v1)
            if(v2 not in g.vs['name']):
                g.add_vertex(v2)
        else:
            g.add_vertex(v1)
            g.add_vertex(v2)
            
        g.add_edge(v1, v2, weight = weight)
    
    print 'graph generated' 
    c = g.community_fastgreedy(weights='weight')
    community = c.as_clustering()
    
    return g, community

def makeCache(keyword, g, community):
    c_num = 0
    for c in community:
        c_num += 1
        if(len(c) >= 5):
            for member in c:
                #makeCache (c_num, member, len(c), g.vs[member]['name'])#, )
                logCache(",".join(keyword), c_num, len(c), g.vs[member]['name'])

def logCache(keyword, cid, csize, content_id):
    query = """
        INSERT INTO recommendation_set(keyword, cid, csize, media, content_id, max_search, weight_threshold)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    print query % (keyword, cid, csize, content_id[0], content_id[2:], MAX_SEARCH, WEIGHT_THRESHOLD)
    cursor.execute(query, (keyword, cid, csize, content_id[0], content_id[2:], MAX_SEARCH, WEIGHT_THRESHOLD))
    conn.commit()

def checkCached(keywords):
    #where = convertWhereStatement("""keyword = "%s" """, keywords)
    where = ",".join(keywords)

    query = """
        SELECT *
        FROM recommendation_set
        WHERE keyword = %s 
        LIMIT 1""" 
    cursor.execute(query, where)
    print query 
    if(cursor.fetchall()):
        return True
    else:
        return False

def getRecommendation(keyword):
    if(checkCached(keyword)):
        return 0 ## return cached content
    else:
        t_dict, i_dict, y_dict = searchKeyword(keyword)
        edge_list = makeGraph(t_dict, i_dict, y_dict)
        graph, community = makeCommunity(edge_list)
        makeCache(keyword, graph, community)

if __name__ == "__main__":
    keywords = ["kimsoohyun"]
    getRecommendation(keywords)
    #print len(edge_list)
    #print len(t_dict), len(i_dict), len(y_dict)

