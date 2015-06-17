#-*- coding: utf-8 -*- 
import sys
import json
import MySQLdb
from igraph import *
# To construct a graph and find communities within it, we used the igraph library

#initialize connection
conn = MySQLdb.connect('localhost', 'root', '**********', 'sns', charset='utf8', use_unicode=True)
cursor = conn.cursor()

"""
1) MAX_SEARCH 
- the number of content used for constructing a graph is too large. 
hence we construct a graph given number of content matched given keyword

2) WEIGHT_THRESHOLD 
- the maximum number of edges of the graph having N vertices is N(N-1)/2, 
which is very large.
- So, we consider the edges between considerablely related 

    [1] WEIGHT_HASH_TAG
    - give weight proportionally as many hashtags are overlapped between two contents

    [2] WEIGHT_MORPHEME
    - we also consider morpheme of text information in social text.
    - but we consider more in order of common noun, foreign language, and proper noun.

"""

MAX_SEARCH = 100 
WEIGHT_THRESHOLD = 5 
WEIGHT_DIFFERENT_MEDIA = 2
WEIGHT_HASH_TAG = 1.5
WEIGHT_MORPHEME = {'NNP': 0.5, 'OL': 0.3, 'NNG': 0.1}
        

"""
    funtion:    getContent
    argument:   content id
    description:
        it returns the content details of given id
        which isn't being used but only for debugging
        T stands for twitter content and I stands for Instagram content,
        and Y stands for Youtube content
"""

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

"""

    funtion:    convertWhereStatement
    argument:   condition, keyword
    description:
        It combines the multiple keyword match for MySQL queries.

"""
def convertWhereStatement(condition, keywords):
    where = []
    for keyword in keywords:
        where.append( "(" + condition + ")")

    where_sentence = " and ".join(where)
    return where_sentence % tuple(keywords)

"""

    funtion:    getCntCommonWords
    argument:   two different lists
    description:
        it simply returns the number of common elements between two lists

"""

def getCntCommonWords(list_a, list_b):
    return len(set(list_a).intersection(set(list_b)))

"""

    funtion:    searchKeyword
    argument:   keywords
    description:
        find the contents matched given keywords.
        1)  YouTube  : matched title and description
        2)  Twitter  : matched tweet message
        3)  Instagram: matched text description

"""

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

"""

    funtion:    getRelation
    argument:   keywords, hashtag, morpheme
    description:
        it caculate the relation score based on the similarities of
        keyword, hashtag, and morpheme between two contents

"""

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

"""

    funtion:    makeGraph
    argument:   twitter, instagram, youtube contents
    description:
        it generates a graph where structure is set of nodes and edges
        nodes are all contents matched given content
        egdes are made when similarity score between two nodes is higher than WEIGHT_THRESHOLD

"""

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

"""

    funtion:    makeCommunity
    argument:   graph
    description:
        it makes set of communities made by fast greedy method, 
        returning the community ids and their corresponding member lists.

"""


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


"""

    funtion:    makeCache
    argument:   keyword, graph, community results
    description:
        It would take much longer time, and give burdens 
        if system makes graph, calculate similarity, and finds communities
        whenever new query comes to.
        Hence we cache the suggestion results under the given queries for a caching purpose by
        simply logs the results.

"""

def makeCache(keyword, g, community):
    c_num = 0
    for c in community:
        c_num += 1
        if(len(c) >= 5): ## we only care the community where the number of members are fairly large enough
            for member in c:
                logCache(",".join(keyword), c_num, len(c), g.vs[member]['name'])

"""

    funtion:    logCache
    argument:   keyword, graph, community results
    description:
        log for cache

"""

def logCache(keyword, cid, csize, content_id):
    query = """
        INSERT INTO recommendation_set(keyword, cid, csize, media, content_id, max_search, weight_threshold)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    print query % (keyword, cid, csize, content_id[0], content_id[2:], MAX_SEARCH, WEIGHT_THRESHOLD)
    cursor.execute(query, (keyword, cid, csize, content_id[0], content_id[2:], MAX_SEARCH, WEIGHT_THRESHOLD))
    conn.commit()


"""

    funtion:    checkCached
    argument:   keyword
    description:
        before analyzing the content and the given keyword
        we first check whether the recommendation result are cached or not.
        *DISCLAMER*
        It caches only primitive information (i.e., keyword)
        However when we consider more information such as timely sensitivity of a content,
        weight threshold, the type of content, and etc, then we could apply more sophisticated
        cache strategy.

"""


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


"""

    funtion:    getRecommendation
    argument:   keyword
    description:
        Basic API to get a recommendation
        it operates under below steps.
            1) searching contents given keyword
            2) making a graph and constructing communities
            3) return the results and make them cached

"""

def getRecommendation(keyword):
    if(checkCached(keyword)):
        return 0 ## return cached content
    else:
        t_dict, i_dict, y_dict = searchKeyword(keyword)
        edge_list = makeGraph(t_dict, i_dict, y_dict)
        graph, community = makeCommunity(edge_list)
        makeCache(keyword, graph, community)

if __name__ == "__main__":
    ## example, keyword can be multiple 
    keywords = ["kimsoohyun"]
    getRecommendation(keywords)

