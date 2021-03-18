from stop_words import stop_words
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

    # Removes &#xa (newline), paragraph and bold tags
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
    # This is done instead of removing DOT from tokens later, as doing this will not fix the problem of words in the
    # end and start of sentences becoming one token. (An example of this would be 'analyzed.my' on post_id = 14)
    doc = doc.replace(".", " ")

    # Removes whitespaces and TAB charaters (\t)
    a_list = doc.split()
    doc = " ".join(a_list)

    filtered = doc
    # Using triple qotes to include qotation marks in the string
    special_chars = """"'#$%&<=>@~()*+-/[]^_`{|}"""
    for character in special_chars:
        filtered = filtered.replace(character, "")

    return filtered


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


# Finds the the edges within the given window size
def create_edges(arr, window_size):
    W = []
    S = []
    for element in arr:
        if len(W) == window_size:
            edges = permutations(W, 2)
            for edge in edges:
                if edge[0] != edge[1]:
                    S.append(edge)
            W.pop(0)
        W.append(element)
    return S


def get_id(tuples, token):
    for id, i in tuples:
        if i == token:
            return id
    return -1


def assign_unique_ids(unique_tuple, tokens):
    # Iterates through all the tokens and finds the unique id for each token
    final = []
    for token in tokens:
        unique = get_id(unique_tuple, token)
        tuple = (unique, token)
        final.append(tuple)
    return final


def remove_dupe_tuples(lst):
    return [t for t in (set(tuple(i) for i in lst))]


def main():

    sc = initializeSpark()

    spark = SparkSession(sc)

    directory, post_id = parse_pls()
    rdds = make_rdds_from_dir(directory, sc)
    post_rdd = rdds["posts_rdd"]

    string = make_stripped_string(post_rdd, post_id)

    print("\n Body from post_id: " + str(post_id) +
          ", stripped of shitespaces and special characters:\n")
    print("'" + string + "'\n")

    # Tokenize the string
    tokens = tokenize(string)
    # remove duplicate entries
    tokens_unique = remove_dupes(tokens)

    # Assign id to the unique tokens
    token_id_tuple = assign_id_to_list(tokens_unique)
    # Now assign these id's the the original token list
    token_id_all = assign_unique_ids(token_id_tuple, tokens)

    print("\nTokens retrieved from the body with their respective id's: \n")
    for i in token_id_all:
        print(i)

    print("\n\nEdges:\n")
    ids = []
    for i in token_id_all:
        ids.append(i[0])

    # Create edges on a window size of 5, using the ids of the tokens
    edges = create_edges(ids, 5)
    # Removes duplicate edges from list
    edges = remove_dupe_tuples(edges)
    print(edges)
    print("\n\nPageRank:")

    sqlContext = SQLContext(sc)

    v = sqlContext.createDataFrame(token_id_tuple, ["id", "word"])

    e = sqlContext.createDataFrame(edges, ["src", "dst"])

    g = graphframes.GraphFrame(v, e)

    results = g.pageRank(resetProbability=0.15, tol=0.0001)
    results.vertices.select("word", "pagerank").show(truncate=False)


if __name__ == "__main__":
    main()
