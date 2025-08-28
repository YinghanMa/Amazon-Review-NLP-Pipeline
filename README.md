# Amazon Review NLP Pipeline  

This project builds a **data pipeline** for processing and analyzing Amazon product reviews.  
It demonstrates skills in **data wrangling, preprocessing, NLP, and visualization**, turning raw semi-structured data into meaningful insights.  

## Project Structure  
```text
Amazon-Review-NLP-Pipeline/
│
├── data_pipeline/
│ ├── amazon_reviews_parsing.py # Parse raw TXT & Excel metadata
│ ├── amazon_reviews_preprocessing.py # Preprocess (tokenize, stopwords, stemming, bigrams)
│
├── notebooks/
│ ├── amazon_reviews_parsing.ipynb
│ ├── amazon_reviews_preprocessing.ipynb
│ └── amazon_reviews_EDA.ipynb # Exploratory Data Analysis
│
├── outputs/
│ ├── parsed_reviews.csv
│ ├── parsed_reviews.json
│ ├── vocab.txt
│ └── countvec.txt
│
├── visuals/
│ ├── rating_distribution.png
│ ├── wordcloud.png
│ ├── bigram_network.png
│ ├── top_tokens.png
│ ├── review_volume.png
│ ├── helpful_votes_vs_length.png
│ └── review_length_by_rating.png
│
├── README.md
```

## Features  

- **Parsing:** Extracts review data from raw TXT and Excel metadata  
- **Preprocessing:** Cleans text (HTML, emojis, invalid Unicode), tokenizes, removes stopwords, stems words, and builds bigrams  
- **Outputs:** Structured CSV/JSON files, vocabulary, and count vectors for NLP tasks  
- **Analysis & Visualization:**  
  - Top frequent tokens  
  - Word clouds  
  - Review length distributions  
  - Review volume trends over years  
  - Relationship between review length and helpful votes  

## Tech Stack  

- **Language:** Python 3  
- **Libraries:** pandas, re, os, datetime, json, nltk, matplotlib, seaborn  
- **Environment:** Jupyter Notebook / Python scripts  

## Example Insights  

- Positive reviews often emphasize *cleaning*, *easy to use*, and *love*.  
- Longer reviews are more likely to receive higher helpful votes.  
- Review volume has increased significantly after 2015.  
- Word clouds reveal product-specific focus such as *water*, *vacuum*, *carpet*, and *machine*.  
