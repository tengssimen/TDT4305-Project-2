

from sw import stop_words
from itertools import permutations
from itertools import islice
import math
import base64
from pyspark.sql import SparkSession
import sys
from sys import argv
import argparse
from pyspark.sql.context import SQLContext
import networkx as nx
import os
import graphframes


def initializeSpark():
    spark = SparkSession.builder.master(
        "local[*]").appName("TDT4305 - Project Part 2").getOrCreate()
    sc = spark.sparkContext
    sc.addPyFile(
        "graphframes-0.8.1-spark3.0-s_2.12.jar")
    sc.addPyFile(
        "graphframes.zip")
    sc.setLogLevel("WARN")
    return sc


def make_rdds_from_dir(directory, sc):
    rdds = {}

    print(
        "\n\nCreating RDD's from directory...\n")

    for filename in os.listdir(directory):
        if filename.endswith(".csv") or filename.endswith(".csv.gz"):
            data_file_name = filename.split('.', 1)[0]
            rdd = sc.textFile(directory + '/' +
                              filename).map(lambda line: line.split("\t"))
            rdd = rdd.mapPartitionsWithIndex(
                lambda a, b: islice(b, 1, None) if a == 0 else b
            )

            rdds[data_file_name + "_rdd"] = rdd

    if(len(rdds) == 0):
        print("\nThere were no CVS files in given directory.\n")
        return rdds

    print("Found " + str(len(rdds)) + " CVS files\n")
    return rdds


def parse_pls():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path")
    parser.add_argument("--post_id")
    args = vars(parser.parse_args())
    directory = args["input_path"]
    post_id = args["post_id"]
    return directory, post_id


def make_stripped_string(post_rdd, post_id):

    post = post_rdd.filter(lambda line: line[0] == post_id)

    # Maps the 6th column to base64
    body = post.map(lambda post: base64.b64decode(post[5])).take(1)

    # Convert to lowercase string
    lowercase = str(body).lower()

    # Removes first three and last 2 characters
    lowercase = lowercase[3:][:-2]

    # Removes &#xa (newline)
    lowercase = lowercase.replace("&#xa", "")

    lowercase = lowercase.replace("<p>", "")
    lowercase = lowercase.replace("</p>", "")
    lowercase = lowercase.replace("<b>", "")
    lowercase = lowercase.replace("</b>", "")

    # Removes punctuations
    punctuations_to_remove = "!,:;?"
    doc = lowercase
    for punctuation in punctuations_to_remove:
        doc = doc.replace(punctuation, "")

    # First replace DOT with whitespace (using whitespace to separate words in beginning and end of sentences)
    # This is done instead of removing DOT from tokens later, as doing this will not fix the problem of words in
    # end and start of sentences becoming one token. (For example 'analyzed.my' on post_id = 14)
    doc = doc.replace(".", " ")

    # Removes whitespaces and TAB charaters (\t)
    a_list = doc.split()
    doc = " ".join(a_list)

    temp = doc
    # Using triple qotes to include qotation marks in the string
    special_chars = """"'#$%&<=>@~()*+-/[]^_`{|}"""
    for character in special_chars:
        temp = temp.replace(character, "")

    return temp


def tokenize(string):

    # Tokenizes the string
    tokens = string.split(" ")

    # Removes tokens with wordlenght < 3
    new_tokens = []
    for token in tokens:
        if len(token) > 2:
            new_tokens.append(token)

    # Remove stopwords
    sw = stop_words()
    word_tokens = new_tokens
    filtered_tokens = [w for w in word_tokens if not w in sw]
    filtered_tokens = []
    for w in word_tokens:
        if w not in sw:
            filtered_tokens.append(w)

    return filtered_tokens


def remove_dupes(tokens):
    unique = []
    for i in tokens:
        if i not in unique:
            unique.append(i)
    return unique


def assign_id_to_list(input):
    tokens = []
    for id, word in enumerate(input):
        tup = (id, word)
        tokens.append(tup)
    return tokens


def slide_over(seq, w):
    num_chunks = ((len(seq) - w)) + 1
    for i in range(0, num_chunks, 1):
        yield seq[i:i + w]


# Function for finding the permutations in a 5 word "window", used for finding edges
def windowSlider(someList):
    W = []
    S = []
    for w in someList:
        if len(W) == 5:
            edges = permutations(W, 2)
            for perm in edges:
                if perm[0] != perm[1]:
                    S.append(perm)
            W.pop(0)
        W.append(w)
    return S


def get_id(tokens, word):
    for id, i in tokens:
        if i == word:
            return id
    return -1


def setIDs(tuple_unike_ord, alle_ord):
    newList = []

    for word in alle_ord:
        unik_id = get_id(tuple_unike_ord, word)
        tuple = (unik_id, word)
        newList.append(tuple)
    return newList


def main():

    sc = initializeSpark()
    # sc.addPyFile(
    # 'https://github.com/graphframes/graphframes/archive/b3b97cf1dac9e7be7806a938c8de406e5c09f431.zip')
    spark = SparkSession(sc)

    directory, post_id = parse_pls()
    rdds = make_rdds_from_dir(directory, sc)
    post_rdd = rdds["posts_rdd"]

    string = make_stripped_string(post_rdd, post_id)

    print("\n Body from post_id: " + str(post_id) +
          ", stripped of shitespaces and special characters:\n")
    print(string + "\n")
    tokens = tokenize(string)
    # remove duplicate entries
    tokens_unique = remove_dupes(tokens)

    # for i in tokens:
    # print(i)

    # print("\n")
    # Assign id to tokens
    token_id_tuple = assign_id_to_list(tokens_unique)
    token_id_all = setIDs(token_id_tuple, tokens)

   # print(token_id_tuple)
   # print("\n")
    # print(token_id_all)

    print("\nTokens retrieved from the body: \n")
    for i in token_id_all:
        print(i)

    # for i in token_id_tuple:
    # print(i)

    print("\n\nSliding window:\n")
    num_slides = 0
    for slice in slide_over(tokens, 5):
        print(num_slides * "\t", slice)
        num_slides = num_slides + 1

    print("\n\nEdges:\n")
    ids = [id[0] for id in token_id_all]
    edges = windowSlider(ids)
    print(edges)
    print("\n\nPageRank:")

    sqlContext = SQLContext(sc)

    v = sqlContext.createDataFrame(token_id_tuple, ["id", "word", ])

    e = sqlContext.createDataFrame(edges, ["src", "dst"])

    g = graphframes.GraphFrame(v, e)

    results = g.pageRank(resetProbability=0.15, tol=0.0001)
    results.vertices.select("id", "pagerank").show(truncate=False)


if __name__ == "__main__":
    main()
