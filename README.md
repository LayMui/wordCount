# 📊 PySpark Word Count

This is a simple **Word Count** program written in Python using **Apache Spark (PySpark)**.  
It demonstrates how to process large text files in parallel and count word frequencies efficiently.  

---

## 🚀 Features
- Reads text data (e.g., from [Project Gutenberg](https://www.gutenberg.org/ebooks/100))  
- Cleans and tokenizes words  
- Removes common stop words  
- Produces word frequency counts  

---

## 🛠️ Requirements
- Python 3.8+  
- [Apache Spark 3.5.x](https://spark.apache.org/downloads.html)  
- Java 11 (required for Spark)  
- pip dependencies:
  ```bash
  pip install pyspark
