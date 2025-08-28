# -*- coding: utf-8 -*-
"""
Amazon Reviews Parsing Script
-----------------------------

This script parses raw Amazon product review data (TXT + Excel metadata),
cleans and normalizes it into structured formats (CSV + JSON).
It is the first step of the Amazon Review NLP Pipeline project.

Functions:
- Load and parse multiple raw TXT review files
- Extract product metadata from Excel
- Clean text fields (remove HTML, emojis, invalid Unicode)
- Normalize timestamps into a standard format
- Export consolidated data into CSV and JSON formats

Environment: Python 3
Libraries used:
- re (regular expressions for cleaning)
- pandas (data manipulation & export)
- os (file system interaction)
- datetime (timestamp standardization)
- json (JSON export)

</div>    

[1. Introduction](#introduction) <br>  
[2. Importing Libraries](#importing-libraries) <br>  
[3. Examining Raw Data](#examining-raw-data) <br>  
[4. Loading and Parsing Files](#-loading-and-parsing-files) <br>  
$\;\;\;\;$[4.1. Defining Regular Expressions](#defining-regular-expressions) <br>  
$\;\;\;\;$[4.2. Reading Files](#reading-files) <br>  
$\;\;\;\;\;\;\;$[4.2.1 Parsing TxT File](#parsing-txt-file) <br>  
$\;\;\;\;\;\;\;$[4.2.2 Parsing Excel File](#parsing-excel-file) <br>  
$\;\;\;\;\;\;\;$[4.2.3 Combined and Further Process](#combined-and-further-process) <br>  
[5. Writing File](#5writing-file) <br>  
$\;\;\;\;$[5.1 Writing to CSV File](#writing-to-csv-file) <br>  
$\;\;\;\;$[5.2 Writing to JSON File](#writing-to-json-file) <br>  
$\;\;\;\;$[5.3 Verification of the Generated JSON File](#verification-of-the-generated-json-file) <br>  
[6. Summary](#summary) <br>  
[7. References](#references) <br>

-------------------------------------

<div class="alert alert-block alert-warning">

## 1.Introduction  <a class="anchor" name="introduction"></a>
    
</div>

This task focuses on extracting structured data from semi-structured Amazon product review files. The dataset includes 14 .txt files and 1 .xlsx file, each containing user review information with 11 attributes. The goal is to parse and clean the data, then generate two outputs:

- A .csv file summarising review counts per parent_product_id

- A .json file grouping all reviews by parent_product_id, with all fields as strings

All fields must follow the specified format strictly, including converting timestamps and handling missing or non-English content. The cleaned data will be used in later tasks for further text analysis.

-------------------------------------

<div class="alert alert-block alert-warning">
    
## 2.Importing Libraries  <a class="anchor" name="importing-libraries"></a>
 </div>

The packages to be used in this assessment are imported in the following. They are used to fulfill the following tasks:

- **re**: to define and apply regular expressions for pattern matching within the text.
- **pandas**: to manage and manipulate structured data using DataFrames.
- **os**: to access and interact with the file system for loading input files.
- **datetime**: to convert and standardise timestamps from raw formats.
- **json**: to load and export data in JSON format for final output.
"""

# Import Necessary Libraries
import pandas as pd
import re
import os
import datetime
import json

"""-------------------------------------

<div class="alert alert-block alert-warning">

## 3.Examining Raw Data <a class="anchor" name="examining-raw-data"></a>

 </div>
"""

from google.colab import drive
drive.mount('/content/drive')

"""Check Excel Raw Data"""

# Load Excel file
excel_path = '/content/drive/MyDrive/student_group168/group168.xlsx'
df_xls = pd.read_excel(excel_path)

# Show the first few rows
df_xls.info()

"""Check TxT Raw Data"""

txt_dir = "/content/drive/MyDrive/student_group168/"
txt_files = [f for f in os.listdir(txt_dir) if f.endswith(".txt")]
print("Number of .txt files:", len(txt_files))

# Read and print the first few lines from one TXT file
txt_path = '/content/drive/MyDrive/student_group168/group168_0.txt'
with open(txt_path, 'r', encoding='utf-8') as f:
    txt_content = f.read()

# Show one full record (just as string)
print(txt_content.split("<record>")[1].split("</record>")[0])

type(txt_content)

"""First of all, the dataset provided for this assessment was located in Google Drive and includes:

- 1 Excel file containing semi-structured tabular data.

- 14 TXT files in pseudo-XML format, each representing review records with 11 fields.

After examining a few sample records, the following structure and patterns were identified:

1.Excel File (group168.xlsx)
- Structured as a table.

- Each row corresponds to one review.

- Columns match the expected 11 attributes (e.g., category, reviewer_id, review_text, parent_product_id, etc.).

- Data types are mixed (e.g., strings, floats, booleans).


2.TXT Files (group168_0.txt ~ group168_13.txt)

- Contain multiple <record> elements enclosed in pseudo-XML.

- Each <record> includes 11 fields but formatting inconsistencies may exist (e.g., tag casing or missing tags).

- Field content may include:

  - Text emojis or invalid symbols (e.g., 😃, �).
  
  - HTML-like tags (e.g., \<br>) inside review_text.

  - Varying timestamp formats.

-------------------------------------

<div class="alert alert-block alert-warning">

## 4.Loading and Parsing Files <a class="anchor" name="loading-and-parsing-files"></a>

</div>

In this section, the raw data files (.txt and .xlsx) are parsed and transformed into structured formats. To handle the irregularities in the txt files, appropriate regular expressions are defined and applied for extraction.
The parsed information includes 11 key attributes such as rating, review text, product ID, etc.

-------------------------------------

<div class="alert alert-block alert-info">
    
### 4.1 Defining Regular Expressions <a class="anchor" name="defining-regular-expressions"></a>

Defining correct regular expressions is crucial in extracting desired information from the text efficiently.

In this task, regular expressions were carefully crafted to extract 11 specific attributes from each review entry. Given the semi-structured nature of the .txt files, where tag names vary in spelling, casing, and spacing (e.g., rating, Rate, rate, review_title, heading), a flexible and inclusive approach was adopted.

To ensure the robustness of the regex patterns, a trial-and-error strategy was applied in Section 4.2.1, where different tag variations were tested and their extraction counts verified. Based on these explorations, the final set of regular expressions was constructed to accurately capture each attribute across all records.
"""

#reg ex pattern for each record
pattern_record = r'<record>(.*?)</record>'
# Category of the product
pattern_category = r"<\s*(?:/?category)\s*>\s*(.*?)\s*<(?:\s*//?category)\s*>"
# Reviewer ID
pattern_reviewer_id = r"<\s*reviewer[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*reviewer[\s_]*id\s*>"
# Rating score
pattern_rating = r"<\s*(?:rate|rating)\s*>\s*(.*?)\s*<\s*/+\s*(?:rate|rating)\s*>"
# Review title or heading
pattern_review_title = r"<\s*(?:review[\s_]*title|heading)\s*>\s*(.*?)\s*<\s*/+\s*(?:review[\s_]*title|heading)\s*>"
# Review text body
pattern_text = r"<\s*(?:review?[\s_]*text|text)\s*>\s*(.*?)\s*<\s*/*\s*(?:review?[\s_]*text|text)\s*>"
# Attached images or pictures
pattern_attached_image = r"<\s*(?:attached[\s_]*images|pictures|pics)\s*>\s*(.*?)\s*<\s*/+\s*(?:attached[\s_]*images|pictures|pics)\s*>"
# Product ID
pattern_product_id = r"<\s*product[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*product[\s_]*id\s*>"
# Parent product ID
pattern_parent_product_id = r"<\s*parent[\s_]*product[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*parent[\s_]*product[\s_]*id\s*>"
# Review timestamp
pattern_timestamp = r"<\s*(?:review[\s_]*timestamp|timestamp|date|time)\s*>\s*(.*?)\s*<\s*/+\s*(?:review[\s_]*timestamp|timestamp|date|time)\s*>"
# Verified purchase tag
pattern_is_verified_purchase = r"<\s*(?:is[\s_]*)?verified[\s_]*purchase\s*>\s*(.*?)\s*<\s*/+\s*(?:is[\s_]*)?verified[\s_]*purchase\s*>"
# Helpful votes or likes
pattern_vote = r"<\s*(?:helpful[\s_]*votes?|votes?|likes)\s*>\s*(.*?)\s*<\s*/+\s*(?:helpful[\s_]*votes?|votes?|likes)\s*>"

"""These patterns are used in the next step when reading the files.

-------------------------------------

<div class="alert alert-block alert-info">
    
### 4.2 Reading Files <a class="anchor" name="reading-files"></a>

In this step, all files are read and parsed.

Let's take a look at the first ten elements of the lists generated. We can see that ids, reviews,etc. are parsed and stored correctly.

<div class="alert alert-block alert-info">
    
#### 4.2.1 Parsing TxT File <a class="anchor" name="parsing-txt-file"></a>

In this section, we locate and load all `.txt` files relevant to our group (`group168_0.txt` to `group168_13.txt`).
We walk through the current working directory and identify all `.txt` files whose names start with "group168_".

All identified files are read and concatenated into a single string named `data`, which will be further processed to extract individual review records.
"""

# Initialize an empty list to store file paths
txt_files = []

# Walk through all folders and files starting from the current directory
for root, _, files in os.walk("."):
    for file in files:
        # Only include .txt files that start with 'group168_'
        if file.endswith(".txt") and file.startswith("group168_"):
            file_path = os.path.join(root, file)
            txt_files.append(file_path)

print(f"Found {len(txt_files)} group168 .txt files.")

# Read the contents of all .txt files and concatenate into a single string
data = "".join(open(path, encoding="utf-8").read() for path in txt_files)

len(data)

type(data)

"""A total of 14 text files were successfully located and read. All content has been concatenated into a single string with over 18 million characters, which will be parsed in the next steps.

-------------------------------------

##### a) Parsing `<record>` Blocks

In this section, we extract all `<record>` blocks from the combined text data using regular expressions.
Each record is then parsed to retrieve 11 specific attributes using the previously defined patterns.

A helper function `parse_record_block()` is created to apply all regex patterns and return a dictionary of extracted fields.
If a field is not found, the value is set to `'none'` to maintain consistency and allow further processing.
"""

# Define a function to extract values using regex patterns
def parse_record_block(record_str):
    def extract(pattern, text):
        match = re.search(pattern, text, re.DOTALL)
        return match.group(1).strip() if match else 'none'

    return {
        'category': extract(pattern_category, record_str),
        'reviewer_id': extract(pattern_reviewer_id, record_str),
        'rating': extract(pattern_rating, record_str),
        'review_title': extract(pattern_title, record_str),
        'review_text': extract(pattern_review_text, record_str),
        'attached_images': extract(pattern_attached_images, record_str),
        'product_id': extract(pattern_product_id, record_str),
        'parent_product_id': extract(pattern_parent_product_id, record_str),
        'review_timestamp': extract(pattern_timestamp, record_str),
        'is_verified_purchase': extract(pattern_verified, record_str),
        'helpful_votes': extract(pattern_votes, record_str)
    }

# Extract all <record> blocks using the previously defined regex
pattern_record = r"<record>(.*?)</record>"
record_list = re.findall(pattern_record, data, re.DOTALL | re.IGNORECASE)

print(f"Total records extracted: {len(record_list)}")

# Join all record strings into a single string
joined_records = "\n".join(record_list)

# Extract all <tag> and </tag> elements
tag_list = re.findall(r"<\s*/?\s*[a-zA-Z0-9_ ]+\s*>", joined_records)

"""-------------------------------------

##### b) Extracting Fields:

###### Extracting Category Field
"""

# Find all opening tags related to 'category'
category_tags = [tag for tag in tag_list if "category" in tag.lower() and not tag.startswith("</")]
print(sorted(set(category_tags)))

# Define a regex pattern to extract the text inside <category> tags
pattern_category = r"<\s*(?:/?category)\s*>\s*(.*?)\s*<(?:\s*//?category)\s*>"
category_pattern = re.compile(pattern_category, re.IGNORECASE)

# extract all 'category' values from the text to check
categories = category_pattern.findall(joined_records)
print(len(categories))

"""Based on the tag analysis, we identified multiple variations of the category tag, such as: `['< /CATEGORY>', '< /CaTEGORY>', '< /Category>', '< /category>', '< CATEGORY>', '< CaTEGORY>', '< Category>', '< category>', '<CATEGORY>', '<CaTEGORY>', '<Category>', '<category>']`.

To handle all possible forms, we constructed a regex pattern based on above variations. After applying the pattern, we extracted 280000 category values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

###### Extracting Reviewer ID
"""

# Find all tags related to 'reviewer_id'
reviewer_id_tags = [
    tag for tag in tag_list
    if "reviewer" in tag.lower() and "id" in tag.lower()
]

print(sorted(set(reviewer_id_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <reviewer_id> tags
pattern_reviewer_id = r"<\s*reviewer[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*reviewer[\s_]*id\s*>"
reviewer_id_pattern = re.compile(pattern_reviewer_id, re.IGNORECASE)

# Extract all 'reviewer_id' values from the text
reviewer_ids = reviewer_id_pattern.findall(joined_records)
print(len(reviewer_ids))

"""Based on the tag analysis, we identified multiple variations of the review_id tag, such as: `['< /ReviewerID>', '< /Reviewer_id>', '< /reviewerID>', '< /reviewer_id>', '< ReviewerID>', '< Reviewer_id>', '< reviewerID>', '< reviewer_id>', '</ReviewerID>', '</Reviewer_id>', '</reviewerID>', '</reviewer_id>', '<ReviewerID>', '<Reviewer_id>', '<reviewerID>', '<reviewer_id>']`.

Similarly, we extracted 280000 review_id values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

###### Extracting Rating
"""

# Find all opening tags related to 'rate'
rate_tags = [
    tag for tag in tag_list
    if "rat" in tag.lower()
    #if any(kw in tag.lower() for kw in ["rate", "rating"])
]
print(sorted(set(rate_tags)))

# Define a regex pattern to extract the text inside <rate> tags
pattern_rate = r"<\s*(?:rate|rating)\s*>\s*(.*?)\s*<\s*/+\s*(?:rate|rating)\s*>"
rate_pattern = re.compile(pattern_rate, re.IGNORECASE)

# Extract all 'rate' values from the text
rates = rate_pattern.findall(joined_records)
print(len(rates))

"""Based on the tag analysis, we identified multiple variations of the rating tag, such as: `['< /Rate>', '< /Rating>', '< /rate>', '< /rating>', '< Rate>', '< Rating>', '< rate>', '< rating>', '</Rate>', '</Rating>', '</rate>', '</rating>', '<Rate>', '<Rating>', '<rate>', '<rating>']`.

Similarly, we extracted 280000 rating values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

###### Extracting Review Title
"""

# Find all tags related to 'review_title'
review_title_tags = [
    tag for tag in tag_list
    if ("review" in tag.lower() and "title" in tag.lower()) or "head" in tag.lower()
]

print(sorted(set(review_title_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <review_title> tags
pattern_review_title = r"<\s*(?:review[\s_]*title|heading)\s*>\s*(.*?)\s*<\s*/+\s*(?:review[\s_]*title|heading)\s*>"
review_title_pattern = re.compile(pattern_review_title, re.IGNORECASE)

# Extract all 'review_title' values from the text
review_titles = review_title_pattern.findall(joined_records)
print(len(review_titles))

"""Based on the tag analysis, we identified multiple variations of the review title tag, such as: `['< /Heading>', '< /Review_title>', '< /heading>', '< /review_title>', '< Heading>', '< Review_title>', '< heading>', '< review_title>', '</Heading>', '</Review_title>', '</heading>', '</review_title>', '<Heading>', '<Review_title>', '<heading>', '<review_title>']`.

Similarly, we extracted 280000 review title values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

######Extracting Review Text
"""

# Find all tags related to 'text'
text_tags = [tag for tag in tag_list if "text" in tag.lower()]

print(sorted(set(text_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <text> tags
pattern_text = r"<\s*(?:review?[\s_]*text|text)\s*>\s*(.*?)\s*<\s*/*\s*(?:review?[\s_]*text|text)\s*>"
text_pattern = re.compile(pattern_text, re.IGNORECASE)

# Extract all 'text' values from the text
texts = text_pattern.findall(joined_records)
print(len(texts))

"""Based on the tag analysis, we identified multiple variations of the review text tag, such as: `['< /Review_text>', '< /review_text>', '< /text>', '< Review_text>', '< review_text>', '< text>', '</Review_text>', '</review_text>', '</text>', '<Review_text>', '<review_text>', '<text>']`.

Similarly, we extracted 280000 review text values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

######Extracting Attached Images
"""

# Find all tags related to 'attached_images'
attached_images_tags = [
    tag for tag in tag_list
    if (("attach" in tag.lower() and "image" in tag.lower()) or "pic" in tag.lower())
]

print(sorted(set(attached_images_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <attached_image> tags
pattern_attached_image = r"<\s*(?:attached[\s_]*images|pictures|pics)\s*>\s*(.*?)\s*<\s*/+\s*(?:attached[\s_]*images|pictures|pics)\s*>"
attached_image_pattern = re.compile(pattern_attached_image, re.IGNORECASE)

# Extract all 'attached_images' values from the text
attached_images = attached_image_pattern.findall(joined_records)
print(len(attached_images))

"""Based on the tag analysis, we identified multiple variations of the attached image tag, such as: `['< /Attached_images>', '< /Pics>', '< /Pictures>', '< /attached_images>', '< /pics>', '< /pictures>', '< Attached_images>', '< Pics>', '< Pictures>', '< attached_images>', '< pics>', '< pictures>', '</Attached_images>', '</Pics>', '</Pictures>', '</attached_images>', '</pics>', '</pictures>', '<Attached_images>', '<Pics>', '<Pictures>', '<attached_images>', '<pics>', '<pictures>']`.

Similarly, we extracted 280000 attached image values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

######Extracting Product ID
"""

# Find all tags related to 'product_id'
product_id_tags = [
    tag for tag in tag_list
    if ("product" in tag.lower() and "id" in tag.lower()) and "parent" not in tag.lower()
]

print(sorted(set(product_id_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <product_id> tags
pattern_product_id = r"<\s*product[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*product[\s_]*id\s*>"
product_id_pattern = re.compile(pattern_product_id, re.IGNORECASE)

# Extract all 'product_ids' values from the text
product_ids = product_id_pattern.findall(joined_records)
print(len(product_ids))

"""Based on the tag analysis, we identified multiple variations of the product id tag, such as: `['< /PRODUCTID>', '< /Product_id>', '< /productID>', '< /product_id>', '< PRODUCTID>', '< Product_id>', '< productID>', '< product_id>', '</PRODUCTID>', '</Product_id>', '</productID>', '</product_id>', '<PRODUCTID>', '<Product_id>', '<productID>', '<product_id>']`.

Similarly, we extracted 280000 product id values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

######Extracting Parent Product ID
"""

# Find all tags related to 'parent_product_id'
parent_product_id_tags = [
    tag for tag in tag_list
    if ("product" in tag.lower() and "id" in tag.lower()) and "parent" in tag.lower()
]

print(sorted(set(parent_product_id_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <parent_product_id> tags
pattern_parent_product_id = r"<\s*parent[\s_]*product[\s_]*id\s*>\s*(.*?)\s*<\s*/*\s*parent[\s_]*product[\s_]*id\s*>"
parent_product_id_pattern = re.compile(pattern_parent_product_id, re.IGNORECASE)

# Extract all 'parent_product_ids' values from the text
parent_product_ids = parent_product_id_pattern.findall(joined_records)
print(len(parent_product_ids))

"""Based on the tag analysis, we identified multiple variations of the parent product id tag, such as: `['< /ParentPRoductID>', '< /Parent_product_id>', '< /parentPRODUCTID>', '< /parent_product_id>', '< ParentPRoductID>', '< Parent_product_id>', '< parentPRODUCTID>', '< parent_product_id>', '</ParentPRoductID>', '</Parent_product_id>', '</parentPRODUCTID>', '</parent_product_id>', '<ParentPRoductID>', '<Parent_product_id>', '<parentPRODUCTID>', '<parent_product_id>']`.

Similarly, we extracted 280000 parent product id values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

######Extracting Timestamp
"""

# Find all tags related to 'timestamp'
timestamp_tags = [
    tag for tag in tag_list
    if "time" in tag.lower() or "date" in tag.lower()
]

print(sorted(set(timestamp_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <timestamp> tags
pattern_timestamp = r"<\s*(?:review[\s_]*timestamp|timestamp|date|time)\s*>\s*(.*?)\s*<\s*/+\s*(?:review[\s_]*timestamp|timestamp|date|time)\s*>"
timestamp_pattern = re.compile(pattern_timestamp, re.IGNORECASE)

# Extract all 'timestamps' values from the text
timestamps = timestamp_pattern.findall(joined_records)
print(len(timestamps))

"""Based on the tag analysis, we identified multiple variations of the timestamp tag, such as: `['< /Date>', '< /Review_timestamp>', '< /Time>', '< /Timestamp>', '< /date>', '< /review_timestamp>', '< /time>', '< /timestamp>', '< Date>', '< Review_timestamp>', '< Time>', '< Timestamp>', '< date>', '< review_timestamp>', '< time>', '< timestamp>', '</Date>', '</Review_timestamp>', '</Time>', '</Timestamp>', '</date>', '</review_timestamp>', '</time>', '</timestamp>', '<Date>', '<Review_timestamp>', '<Time>', '<Timestamp>', '<date>', '<review_timestamp>', '<time>', '<timestamp>']`.

Similarly, we extracted 280000 timestamp values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

###### Extracting Verified Purchase Status
"""

# Find all tags related to 'is_verified_purchase'
is_verified_purchase_tags = [
    tag for tag in tag_list
    if "verif" in tag.lower()
]

print(sorted(set(is_verified_purchase_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <is_verified_purchase> tags
pattern_is_verified_purchase = r"<\s*(?:is[\s_]*)?verified[\s_]*purchase\s*>\s*(.*?)\s*<\s*/+\s*(?:is[\s_]*)?verified[\s_]*purchase\s*>"
is_verified_purchase_pattern = re.compile(pattern_is_verified_purchase, re.IGNORECASE)

# Extract all 'is_verified_purchases' values from the text
is_verified_purchases = is_verified_purchase_pattern.findall(joined_records)
print(len(is_verified_purchases))

"""Based on the tag analysis, we identified multiple variations of the verified purchase tag, such as: `['< /Is_verified_purchase>', '< /Verified_purchase>', '< /is_verified_purchase>', '< /verified_purchase>', '< Is_verified_purchase>', '< Verified_purchase>', '< is_verified_purchase>', '< verified_purchase>', '</Is_verified_purchase>', '</Verified_purchase>', '</is_verified_purchase>', '</verified_purchase>', '<Is_verified_purchase>', '<Verified_purchase>', '<is_verified_purchase>', '<verified_purchase>']`.

Similarly, we extracted 280000 verified purchase values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

###### Extracting Helpful Votes
"""

# Find all tags related to 'vote'
vote_tags = [
    tag for tag in tag_list
    if "vote" in tag.lower() or "like" in tag.lower()
]

print(sorted(set(vote_tags)))  # Show all tag variations found

# Define a regex pattern to extract the text inside <vote> tags
pattern_vote = r"<\s*(?:helpful[\s_]*votes?|votes?|likes)\s*>\s*(.*?)\s*<\s*/+\s*(?:helpful[\s_]*votes?|votes?|likes)\s*>"
vote_pattern = re.compile(pattern_vote, re.IGNORECASE)

# Extract all 'vote' values from the text
votes = vote_pattern.findall(joined_records)
print(len(votes))

"""Based on the tag analysis, we identified multiple variations of the helpful vote tag, such as: `['< /Helpful_votes>', '< /Likes>', '< /Votes>', '< /helpful_vote>', '< /helpful_votes>', '< /likes>', '< /votes>', '< Helpful_votes>', '< Likes>', '< Votes>', '< helpful_vote>', '< helpful_votes>', '< likes>', '< votes>', '</Helpful_votes>', '</Likes>', '</Votes>', '</helpful_vote>', '</helpful_votes>', '</likes>', '</votes>', '<Helpful_votes>', '<Likes>', '<Votes>', '<helpful_vote>', '<helpful_votes>', '<likes>', '<votes>']`.

Similarly, we extracted 280000 helpful vote values, which matches the total number of records, confirming that our regular expression works as expected.

-------------------------------------

##### c) Verifying Regular Expression Coverage

To ensure our regular expression captures all tag fields, we tested the pattern against every record. The code below checks for unmatched records and displays a few examples for debugging.
"""

pattern_text = re.compile(
    r"<\s*(?:review?[\s_]*text|text)\s*>\s*(.*?)\s*<\s*/*\s*(?:review?[\s_]*text|text)\s*>",
    re.IGNORECASE)

unmatched_records = []
for r in record_list:
    if not pattern_text.search(r):
        unmatched_records.append(r)

print(f"Unmatched records (no rate found): {len(unmatched_records)}")

for r in unmatched_records[:3]:
    print(r)
    print("="*80)

"""-------------------------------------

##### d) Creating DataFrame

After successfully extracting all required fields using regular expressions, we construct a DataFrame (`df_txt`) that stores the cleaned review data. This DataFrame includes 11 attributes as required in the output specification.

To ensure consistency and compliance with format requirements, several type conversions are applied:

- `rating` and `helpful_votes` are converted to numeric types.
- `review_timestamp` is parsed from Unix timestamp (milliseconds) into the required format `YYYY-MM-DD HH:MM:SS`.
- `is_verified_purchase` is converted to Boolean based on lowercase string comparison.

These transformations guarantee the validity of values before outputting to JSON and CSV formats.
"""

df_txt = pd.DataFrame({
    "category": categories,
    "reviewer_id": reviewer_ids,
    "rating": rates,
    "review_title": review_titles,
    "review_text": texts,
    "attached_images": attached_images,
    "product_id": product_ids,
    "parent_product_id": parent_product_ids,
    "review_timestamp": timestamps,
    "is_verified_purchase": is_verified_purchases,
    "helpful_votes": votes
})

# 类型转换
df_txt["rating"] = pd.to_numeric(df_txt["rating"], errors="coerce")
df_txt["helpful_votes"] = pd.to_numeric(df_txt["helpful_votes"], errors="coerce")
df_txt["review_timestamp"] = pd.to_datetime(df_txt["review_timestamp"], unit="ms", utc=True)
df_txt["review_timestamp"] = df_txt["review_timestamp"].dt.strftime('%Y-%m-%d %H:%M:%S')
df_txt["is_verified_purchase"] = df_txt["is_verified_purchase"].str.strip().str.lower() == "true"

df_txt

"""-------------------------------------

<div class="alert alert-block alert-info">
    
#### 4.2.2 Parsing Excel File <a class="anchor" name="parsing-excel-file"></a>

In addition to the `.txt` files, the dataset includes an `.xlsx` file (`group168.xlsx`) containing review data. All sheets within the Excel file were loaded, cleaned, and concatenated into a single DataFrame. Unnecessary columns starting with `'X'` were dropped, and the timestamp column was formatted into the required `YYYY-MM-DD HH:MM:SS` string format for consistency with the `.txt` data.
"""

# Load the Excel file
xls = pd.ExcelFile('/content/drive/MyDrive/student_group168/group168.xlsx')

# Retrieve the list of all sheet names present in the Excel file.
xls.sheet_names

# Read all sheets into a list of DataFrames
# Each sheet is loaded individually using its name
all_sheets = [pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names]

# Combine all individual DataFrames into a single DataFrame
df_combined_xls = pd.concat(all_sheets, ignore_index=True)

# Check
df_combined_xls.info()

# Drop unwanted columns that start with 'X' and are entirely empty
df_combined_xls.drop(columns=[col for col in df_combined_xls.columns if col.startswith('X')], inplace=True)

# Convert the 'review_timestamp' column to datetime format.
df_combined_xls['review_timestamp'] = pd.to_datetime(df_combined_xls['review_timestamp'], unit='ms', utc=True)
# Convert the 'review_timestamp' column to string format: 'YYYY-MM-DD HH:MM:SS'
df_combined_xls['review_timestamp'] = df_combined_xls['review_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')

df_combined_xls.info()

df_combined_xls.head()

"""-------------------------------------

<div class="alert alert-block alert-info">
    
#### 4.2.3 Combined and Further Process <a class="anchor" name="combined-and-further-process"></a>
"""

# Combine Excel and TXT data
df_combined = pd.concat([df_combined_xls, df_txt], ignore_index=True)
df_combined.info()

# Clean the "review_text" column
# Remove emojis using Unicode ranges (e.g., emoticons, flags, CJK symbols)
def remove_emojis(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002500-\U00002BEF"  # CJK characters
        "\U00002702-\U000027B0"  # dingbats
        "\U000024C2-\U0001F251"  # enclosed characters
        "]+", flags=re.UNICODE)
    return emoji_pattern.sub(r'', text)

# Remove HTML tags using regex
def remove_html_tags(text):
    return re.sub(r'<.*?>', '', text)

# Remove unreadable or invalid symbols (e.g., �, □, etc.)
def remove_invalid_symbols(text):
    return re.sub(r'[^\x20-\x7F]+', '', text)

# Combine all cleaning functions into one
def clean_review(text):
    text = str(text)
    text = remove_html_tags(text)
    text = remove_emojis(text)
    text = remove_invalid_symbols(text)
    return text.strip()

# Check if a string is entirely ASCII (i.e., English)
def is_entirely_english(text):
    try:
        text.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False

# Clean the "review_text" and remove non-English entries
df_combined["review_text"] = df_combined["review_text"].apply(clean_review)
df_combined["review_text"] = df_combined["review_text"].apply(
    lambda x: x if is_entirely_english(x) else "none"
)

# Standardize all fields
# Replace missing values and empty strings with "none"
df_combined.fillna("none", inplace=True)
df_combined.replace("", "none", inplace=True)

# Identify all object-type columns (text fields)
text_cols = df_combined.select_dtypes(include='object').columns

# Convert all text fields to lowercase
for col in text_cols:
    df_combined[col] = df_combined[col].astype(str).str.lower()

# Remove duplicate rows after cleaning
df_combined.drop_duplicates(inplace=True)

df_combined.info()

df_combined

"""-------------------------------------

<div class="alert alert-block alert-info">
    
## 5.Writing File <a class="anchor" name="writing-file"></a>

<div class="alert alert-block alert-info">
    
##### 5.1  Writing to CSV File <a class="anchor" name="writing-to-csv-file"></a>

Generated a summary table with total reviews and valid text reviews for each parent_product_id, then saved as a UTF-8 encoded CSV file.
"""

# Count total number of reviews for each parent_product_id
review_counts = df_combined.groupby("parent_product_id").size().reset_index(name="review_count")

# Count number of reviews that contain text (excluding 'none')
text_counts = df_combined[df_combined["review_text"] != "none"] \
    .groupby("parent_product_id").size().reset_index(name="review_text_count")

# Merge the two counts into a single DataFrame
csv_output = pd.merge(review_counts, text_counts, on="parent_product_id", how="left")

# Replace NaN with 0 and ensure integers
csv_output = csv_output.fillna(0).astype({"review_text_count": int})

# Convert to string
csv_output["parent_product_id"] = csv_output["parent_product_id"].astype(str)

# Save to CSV file in utf-8 encoding
csv_output.to_csv("task1_group_168.csv", index=False, encoding="utf-8")

from google.colab import files
files.download("task1_group_168.csv")

"""-------------------------------------

<div class="alert alert-block alert-warning">

### 5.2 Writing to JSON File <a class="anchor" name="writing-to-json-file"></a>

</div>

Grouped reviews by parent_product_id, stored them under "reviews", and saved the result in a formatted JSON file.
"""

# Convert all fields to string type to meet JSON format requirements
df_json_ready = df_combined.astype(str)

# Build a nested JSON structure grouped by parent_product_id
json_data = {}
for parent_id, group in df_json_ready.groupby("parent_product_id"):
    # Drop the group key column and convert remaining rows to dictionaries
    reviews = group.drop(columns=["parent_product_id"]).to_dict(orient="records")
    # Assign the list of reviews under the corresponding parent ID
    json_data[parent_id] = {"reviews": reviews}

# Write the JSON data to file using UTF-8 encoding
with open("task1_group_168.json", "w", encoding="utf-8") as f:
    json.dump(json_data, f, ensure_ascii=False, indent=2)

from google.colab import files
files.download("task1_group_168.json")

"""-------------------------------------

<div class="alert alert-block alert-info">
    
### 5.3. Verification of the Generated JSON File <a class="anchor" name="verification-of-the-generated-json-file"></a>

To ensure the generated JSON file follows the correct structure, we included a helper function to validate:

- Whether the file can be successfully loaded.

- Whether the top-level format is a dictionary.

- Whether each parent_product_id contains a reviews list.

- Whether each review contains exactly 11 required fields with valid types and formats.
"""

import json
from datetime import datetime

def verify_json(filepath):
    try:
        # Try to load the JSON file with UTF-8 encoding
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
        print("Successfully loaded JSON file")
    except json.JSONDecodeError as e:
        print(f"JSON syntax error: {e}")
        return False
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return False
    except Exception as e:
        print(f"Failed to load JSON file: {e}")
        return False

    # Check if the top-level object is a dictionary
    if not isinstance(data, dict):
        print("Top-level JSON structure must be a dictionary")
        return False

    validation_errors = 0
    total_reviews = 0

    for pid, content in data.items():
        # Each key should map to a dictionary with "reviews" as a list
        if not isinstance(content, dict):
            print(f"Value for parent_product_id '{pid}' must be a dictionary")
            validation_errors += 1
            continue

        if "reviews" not in content:
            print(f"Missing 'reviews' key for parent_product_id: '{pid}'")
            validation_errors += 1
            continue

        reviews = content["reviews"]
        if not isinstance(reviews, list):
            print(f"'reviews' must be a list under parent_product_id: '{pid}'")
            validation_errors += 1
            continue

        if len(reviews) == 0:
            print(f"Warning: No reviews found for parent_product_id: '{pid}'")
            continue

        for review_idx, review in enumerate(reviews):
            total_reviews += 1
            if not isinstance(review, dict):
                print(f"Review #{review_idx} under parent_product_id '{pid}' must be a dictionary")
                validation_errors += 1
                continue

            # Check that all expected keys are present
            expected_keys = {
                "category", "reviewer_id", "rating", "review_title", "review_text",
                "attached_images", "product_id", "review_timestamp",
                "is_verified_purchase", "helpful_votes"
            }

            missing_keys = expected_keys - set(review.keys())
            extra_keys = set(review.keys()) - expected_keys

            if missing_keys:
                print(f"Missing keys in review #{review_idx} under parent_product_id '{pid}': {missing_keys}")
                validation_errors += 1

            if extra_keys:
                print(f"Extra keys in review #{review_idx} under parent_product_id '{pid}': {extra_keys}")
                validation_errors += 1

            if missing_keys or extra_keys:
                continue

            # Check that all values are strings
            for k, v in review.items():
                if not isinstance(v, str):
                    print(f"Value of '{k}' must be a string (found {type(v).__name__}) in review #{review_idx} under parent_product_id '{pid}'")
                    validation_errors += 1

            # Check if the timestamp has correct format: YYYY-MM-DD HH:MM:SS
            try:
                datetime.strptime(review["review_timestamp"], "%Y-%m-%d %H:%M:%S")
            except ValueError as e:
                print(f"Invalid timestamp format: '{review['review_timestamp']}' in review #{review_idx} under parent_product_id '{pid}'")
                validation_errors += 1

            # Check if rating is a valid number string
            try:
                rating = float(review["rating"])
                if rating < 0 or rating > 5:
                    print(f"Warning: Rating value '{rating}' outside expected range (0-5) in review #{review_idx} under parent_product_id '{pid}'")
            except ValueError:
                print(f"Rating '{review['rating']}' is not a valid number in review #{review_idx} under parent_product_id '{pid}'")
                validation_errors += 1

    if validation_errors == 0:
        print(f"JSON verification successful! Found {len(data)} products with {total_reviews} reviews total.")
        return True
    else:
        print(f"JSON verification failed with {validation_errors} errors. Found {len(data)} products with {total_reviews} reviews total.")
        return False

# Run the verification
verify_json("task1_group168.json")

"""-------------------------------------

<div class="alert alert-block alert-warning">

## 6.Summary <a class="anchor" name="summary"></a>

</div>

This notebook presents a complete pipeline for extracting, cleaning, and preparing user review data from `.txt` and `.xlsx` files. The key steps and results are summarized below:

Raw Data Extraction:
  - Extracted 284,200 raw records in total:
    - 280000 records from 14 .txt files
    - 4200 records from 1 .xlsx file

Regex-based Parsing:
- Used flexible regular expressions to handle tag variations and ensure robust field extraction across all sources.

Data Cleaning:
- Removed emojis, HTML tags, unreadable characters, and non-English content from review_text.

Standardization:
- Filled missing values with "none", normalized text to lowercase, and converted fields to appropriate types.

Final Dataset:
- After cleaning and deduplication, 278238 valid records were retained.

Export & Validation:
- Exported results to both CSV and JSON formats. A helper function was included to validate the JSON structure.

-------------------------------------

<div class="alert alert-block alert-warning">

## 7.References <a class="anchor" name="references"></a>

</div>

[1]<a class="anchor" name="ref-2"></a> Why do I need to add DOTALL to python regular expression to match new line in raw string, https://stackoverflow.com/questions/22610247, Accessed 30/08/2022.

[2]<a class="anchor" name="ref-2"></a>Python Docs. re — Regular expression operations, https://docs.python.org/3/library/re.html (Accessed: 08 April 2025)

## --------------------------------------------------------------------------------------------------------------------------
"""
