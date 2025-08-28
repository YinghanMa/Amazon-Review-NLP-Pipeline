# -*- coding: utf-8 -*-
"""
Amazon Reviews Preprocessing Script
-----------------------------------

This script performs preprocessing on parsed Amazon review data,
including text normalization, tokenization, stopword removal, stemming,
and n-gram generation. The output vocabulary and count vectors are used
as inputs for downstream NLP tasks such as classification or topic modeling.

Functions:
- Normalize and clean review text
- Tokenize text into words
- Remove stopwords and rare/short tokens
- Apply stemming to reduce words to their root forms
- Build vocabulary (unigrams + bigrams)
- Generate count vector representation (sparse format)

Environment: Python 3
Libraries used:
- os (file system interaction)
- pandas (data manipulation & export)
- multiprocessing (parallel processing of reviews)
- itertools (efficient iteration for bigrams)
- nltk (tokenization and stemming)
    
## Table of Contents

</div>

[1. Introduction](#Intro) <br>
[2. Importing Libraries](#libs) <br>
[3. Examining Input File](#examine) <br>
[4. Loading and Parsing Files](#load) <br>
$\;\;\;\;$[4.1. Tokenization](#tokenize) <br>
$\;\;\;\;$[4.2. Genegrate numerical representation](#whetev1) <br>
[5. Writing Output Files](#write) <br>
$\;\;\;\;$[5.1. Vocabulary List](#write-vocab) <br>
$\;\;\;\;$[5.2. Sparse Matrix](#write-sparseMat) <br>
[6. Summary](#summary) <br>
[7. References](#Ref) <br>

<div class="alert alert-block alert-success">
    
## 1.  Introduction  <a class="anchor" name="Intro"></a>

This assessment concerns textual data and the aim is to extract data, process them, and transform them into a proper format. The dataset provided is in the format of a PDF file containing:

  - category: Product category
  -reviewer_id: Anonymous ID of the reviewe
  -rating: Numerical rating
  -review_title: Short summary of the review
  -review_text: Full content of the user's feedback
  -attached_images: Whether an image is included
  - product_id & parent_product_id: Unique identifiers for product and parent group
  -review_timestamp: Date and time when the review was posted
  -is_verified_purchase: Indicates if the reviewer actually purchased the item
  -helpful_votes: Number of users who found the review helpful

<div class="alert alert-block alert-success">
    
## 2.  Importing Libraries  <a class="anchor" name="libs"></a>

In this assessment, any python packages is permitted to be used. The following packages were used to accomplish the related tasks:

* **os:** to interact with the operating system, e.g. navigate through folders to read files
* **re:** to define and use regular expressions
* **pandas:** to work with dataframes
* **multiprocessing:** to perform processes on multi cores for fast performance
* **collections:** to use defaultdict and Counter for counting and grouping operations
* **math:** to apply logarithmic functions such as log2 during PMI calculations
* **nltk.stem:** to apply Porter stemming and reduce tokens to their root form
* **nltk.collocations:** to find frequently co-occurring word pairs using BigramCollocationFinder and BigramAssocMeasures
"""

import json
import pandas as pd
import re
from nltk.stem import PorterStemmer
from collections import defaultdict
from math import log2
from collections import Counter
from collections import defaultdict
from nltk.collocations import BigramCollocationFinder, BigramAssocMeasures
from collections import Counter

"""-------------------------------------

<div class="alert alert-block alert-success">
    
## 3.  Examining Input File <a class="anchor" name="examine"></a>
"""

from google.colab import drive
drive.mount('/content/drive')

"""Let's examine what is the content of the file. For this purpose, print the contents of the first two."""

with open("/content/drive/MyDrive/task1_group_168.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# View the contents of the first two parent_product_id in the data:
for i, (pid, content) in enumerate(data.items()):
    print(f"Parent Product ID: {pid}")
    print("Reviews Example:")
    for review in content.get("reviews", [])[:2]:
        for k, v in review.items():
            print(f"{k}: {v}")
        print("-" * 30)
    if i >= 1:  # Only 2
        break

"""It is noticed that the file contains nested JSON structures, where each `parent_product_id` maps to multiple review records.

Each review entry includes key fields such as `rating`, `review_text`, `review_title`, `review_timestamp`, `is_verified_purchase`, and `helpful_votes`.

Having parsed the pdf file, the following observations can be made: ....
Having parsed the file, the following observations can be made:

- Most reviews have 5-star ratings, indicating a general trend of user satisfaction.
- Some products contain both positive and negative feedback, as seen in review text variations.
- The `is_verified_purchase` field can be useful for analyzing review credibility.
- The `review_timestamp` allows for potential time-based trend analysis.
- The field `attached_images` is often marked as "none", suggesting image-based analysis may be limited.

<div class="alert alert-block alert-success">
    
## 4.  Loading and Parsing File <a class="anchor" name="load"></a>

In this section, we perform text extraction from the original dataset.
We load the task1_group168.json file, which contains the full review dataset.

The goal is to extract parent_product_id and the corresponding review_text for each entry.

We filter out reviews where review_text is "none" to retain only valid records.
This step ensures that the reviews we process later are meaningful and not missing.
"""

with open("/content/drive/MyDrive/task1_group_168.json") as f:
    data = json.load(f)

records = []
for parent_id, group in data.items():
    for review in group.get("reviews", []):
        records.append({
            "parent_product_id": parent_id,
            "review_text": review.get("review_text", "none")
        })

df = pd.DataFrame(records)

print("Total number of original records:", df.shape[0])
print(df.head())

df_valid = df[df["review_text"].str.lower() != "none"]
print("Total number of valid review_text:", df_valid.shape[0])

"""Let's examine the dictionary generated. For counting the total number of reviews extracted, we identify products with a sufficient number of reviews.

We group the valid review data by parent_product_id and retain only those products that have at least 50 reviews, as required by the task guideline.

This ensures that our analysis is based on robust and representative review samples.

The filtered dataset is stored in df_task2, which will be used for further token processing.
"""

valid_parents = df_valid.groupby("parent_product_id") \
                        .filter(lambda x: len(x) >= 50)["parent_product_id"] \
                        .unique()

print("Number of parent_product_ids with ≥50 reviews:", len(valid_parents))

df_task2 = df_valid[df_valid["parent_product_id"].isin(valid_parents)].copy()

print("The number of records after final filtering:", df_task2.shape)
print(df_task2[["parent_product_id", "review_text"]].head())

"""<div class="alert alert-block alert-warning">
    
### 4.1 Tokenization <a class="anchor" name="tokenize"></a>

Tokenization is a principal step in text processing and producing unigrams. In this section, we clean and tokenize the review texts.

We begin by loading a list of independent stopwords from a file, then initialize a regex pattern to extract words.

Each review text is converted to lowercase, tokenized using the regex, filtered to remove stopwords and short words (less than 3 characters), and stemmed using PorterStemmer.

The final tokens are stored in a dictionary where each key is a parent_product_id and the value is a list of cleaned tokens.
"""

# Load stopwords_en.txt
with open("/content/drive/MyDrive/stopwords_en.txt", "r", encoding="utf-8") as f:
    stopwords_indep = set(f.read().splitlines())

# Initialization
stemmer = PorterStemmer()
token_pattern = re.compile(r"[a-zA-Z]+")

# Build: Each parent_product_id → token list
product_tokens = defaultdict(list)

for _, row in df_task2.iterrows():
    parent_id = row["parent_product_id"]
    text = row["review_text"].lower()

    tokens = token_pattern.findall(text)  # Regular word segmentation
    tokens = [t for t in tokens if t not in stopwords_indep]  # Remove short words + stemming
    tokens = [stemmer.stem(t) for t in tokens if len(t) >= 3]  # Remove independent stopwords

    product_tokens[parent_id].extend(tokens)

"""The above operation results in a dictionary with PID representing keys and a single string for all reviews of the day concatenated to each other.

We count how frequently each token appears across different products.
We determine how many unique products each token appears in.

Tokens that appear in too many products (≥95%) or too few (≤5%) are filtered out, as these are considered either too generic or too rare.

This step helps us retain contextually meaningful and discriminative tokens, which will improve the quality of downstream analyses.
"""

# Count how many parent_product_ids each token appears in
token_product_occurrence = defaultdict(set)

for parent_id, tokens in product_tokens.items():
    for token in set(tokens):  # # Use set to remove duplicates to prevent the same product from being counted twice
        token_product_occurrence[token].add(parent_id)

# Convert to token -> Number of products that appear
token_df = pd.DataFrame([
    {"token": token, "product_count": len(pid_set)}
    for token, pid_set in token_product_occurrence.items()
])

# Get the total number of products
total_products = len(product_tokens)

# Setting the Threshold
high_thresh = 0.95 * total_products
low_thresh = 0.05 * total_products

# Filter out:
# Appearing in 95%+ of products (context-dependent stopwords)
# Appearing in 5%- of products (rare tokens)
filtered_tokens = token_df[
    (token_df["product_count"] < high_thresh) &
    (token_df["product_count"] >= low_thresh)
]["token"].tolist()

print(f"Number of remaining tokens after filtering: {len(filtered_tokens)}")

"""At this stage, all reviews for each PID are tokenized and are stored as a value in the new dictionary (separetely for each day).

-------------------------------------

<div class="alert alert-block alert-warning">
    
### 4.2 Generate numerical representation<a class="anchor" name="bigrams"></a>

One of the tasks is to generate the numerical representation for all tokens in abstract.

In this section, we generate bigram tokens and calculate their significance using the Pointwise Mutual Information (PMI) metric.

We apply frequency filtering to remove rare bigrams and extract the top 200 most significant bigrams.

These are then formatted and added to the vocabulary for later vector representation.
"""

# Prepare the data
tokenized_reviews = []

for _, row in df_task2.iterrows():
    text = row["review_text"].lower()
    tokens = token_pattern.findall(text)
    tokens = [t for t in tokens if t not in stopwords_indep]
    tokens = [stemmer.stem(t) for t in tokens if len(t) >= 3]
    tokens = [t for t in tokens if t in filtered_tokens]
    if tokens:
        tokenized_reviews.append(tokens)

# Constructing PMI model using BigramCollocationFinder
finder = BigramCollocationFinder.from_documents(tokenized_reviews)

# Filter out low frequency bigram
finder.apply_freq_filter(2)

# Extract the first 200 PMI bigrams
pmi = BigramAssocMeasures()
top_bigrams = finder.nbest(pmi.pmi, 200)

# Format Conversion
top_bigrams_str = [f"{w1}_{w2}" for w1, w2 in top_bigrams]

print("Top 5 PMI bigrams (NLTK):")
for i, b in enumerate(top_bigrams_str[:5], 1):
    print(f"{i}. {b}")

"""At this stage, we have a dictionary of tokenized words, whose keys are indicative of the most informative unigrams and bigrams appearing across product reviews. These tokens—refined through stopword filtering, stemming, and PMI-based selection—form the foundation of our final vocabulary. This vocabulary will later be used to construct the sparse matrix representation for each product.

-------------------------------------

<div class="alert alert-block alert-success">
    
## 5. Writing Output Files <a class="anchor" name="write"></a>

Files need to be generated:
* Vocabulary list
* Sparse matrix (count_vectors)

This is performed in the following sections.

<div class="alert alert-block alert-warning">
    
### 5.1 Vocabulary List <a class="anchor" name="write-vocab"></a>

List of vocabulary should also be written to a file, sorted alphabetically, with their reference codes in front of them. This file also refers to the sparse matrix in the next file.

For this purpose, we merge the filtered unigrams and top bigrams, sort the combined list alphabetically, and reassign each token a new index.

The final vocabulary is saved into vocab.txt, which is referenced when creating the sparse representation in the next step.
"""

# Merge and sort alphabetically
vocab_all = sorted(filtered_tokens + top_bigrams_str)

# Reassign indices to each token
with open("group_168_vocab.txt", "w", encoding="utf-8") as f:
    for idx, token in enumerate(vocab_all):
        f.write(f"{token}:{idx}\n")

print(f"The alphabetically sorted vocabulary has been saved to vocab.txt, total {len(vocab_all)} items.")

from google.colab import files
files.download("group_168_vocab.txt")

"""<div class="alert alert-block alert-warning">
    
### 5.2 Sparse Matrix <a class="anchor" name="write-sparseMat"></a>

For writing sparse matrix for a paper, we firstly calculate the frequency of words for that paper.

In this section, we build the countvec.txt file by constructing a sparse matrix.

Each product's reviews are combined, tokenized, and processed into unigrams and bigrams.

The frequency of each token is recorded using the index mapping from the vocabulary file, and the final matrix is saved in the format required for downstream machine learning or text analysis.
"""

# Create token → index mapping (based on vocab.txt)
token_to_index = {token: idx for idx, token in enumerate(vocab_all)}

# Group reviews by product and count token frequencies
product_vectors = {}

for parent_id, group in df_task2.groupby("parent_product_id"):
    # Concatenate all review texts for this product
    all_text = " ".join(group["review_text"].str.lower().tolist())

    # Tokenize and stem
    tokens = token_pattern.findall(all_text)
    tokens = [t for t in tokens if t not in stopwords_indep]
    tokens = [stemmer.stem(t) for t in tokens if len(t) >= 3]

    # Also generate bigrams
    unigrams = [t for t in tokens if t in token_to_index]
    bigrams = [f"{w1}_{w2}" for w1, w2 in zip(tokens, tokens[1:]) if f"{w1}_{w2}" in token_to_index]

    # Combine all tokens and count
    all_used_tokens = unigrams + bigrams
    freq = Counter(all_used_tokens)

    # Convert to sparse string: "index:count"
    sparse_items = [f"{token_to_index[token]}:{count}" for token, count in sorted(freq.items(), key=lambda x: token_to_index[x[0]])]
    product_vectors[parent_id] = sparse_items

# Write to countvec.txt
with open("group_168_countvec.txt", "w", encoding="utf-8") as f:
    for parent_id, sparse_items in product_vectors.items():
        line = f"{parent_id}," + ",".join(sparse_items)
        f.write(line + "\n")

print(f"Sparse representation saved to countvec.txt — {len(product_vectors)} lines.")

from google.colab import files
files.download("group_168_countvec.txt")

"""-------------------------------------

<div class="alert alert-block alert-success">
    
## 6. Summary <a class="anchor" name="summary"></a>

Through a structured process of review text extraction, preprocessing, and frequency analysis, two key output files were generated:

**vocab.txt** (Total: 2,246 items)

  - This file contains an alphabetically sorted list of both unigrams and bigrams extracted from helpful product reviews. Each token is paired with a unique index, which serves as a reference for building the sparse matrix.

  - Contents:

    - Unigrams and PMI-selected bigrams

    - Sorted alphabetically

    - Indexed for easy lookup

**countvec.txt** (Total: 434 lines)

  - This file represents the sparse matrix format, where each line corresponds to a unique parent_product_id and lists the frequency of associated tokens in the format token_index:count.

  - Contents:

    - Token frequencies by product

    - Compact representation for downstream modeling or clustering

These two files together enable numerical representation of textual data, which can be used for various tasks such as similarity detection, product clustering, or further machine learning applications.

-------------------------------------

<div class="alert alert-block alert-success">
    
## 7. References <a class="anchor" name="Ref"></a>

[1] Pandas dataframe.drop_duplicates(), https://www.geeksforgeeks.org/python-pandas-dataframe-drop_duplicates/, Accessed 13/08/2022.

## --------------------------------------------------------------------------------------------------------------------------
"""
