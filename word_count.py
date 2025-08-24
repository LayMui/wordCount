from pyspark import SparkContext, SparkConf
import re

def main():
    # Stop words curated list
    stop_words = {
        'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with',
        'by', 'from', 'up', 'about', 'into', 'through', 'during', 'before',
        'after', 'above', 'below', 'between', 'among', 'throughout', 'despite',
        'towards', 'upon', 'concerning', 'a', 'an', 'as', 'are', 'was', 'were',
        'been', 'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'ought', 'i',
        'you', 'he', 'she', 'it', 'we', 'they', 'them', 'their', 'what', 'which',
        'who', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each',
        'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
        'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'just',
        'don', 'now', 'than', 'that', 'this', 'these', 'those', 'is', 'am',
        'my', 'me', 'him', 'her', 'his', 'hers', 'our', 'ours', 'your', 'yours'
    }

    print("=== PySpark Word Count Demo ===\n")

    # Create SparkContext safely
    conf = SparkConf().setAppName("WordCountExample").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf=conf)
    sc.setLogLevel("ERROR")  # reduce verbose logging
    # Read the file into an RDD
    baseRDD = sc.textFile("shakespeare.txt")

    # Split lines into words
    wordsRDD = baseRDD.flatMap(lambda line: re.findall(r'\b[a-zA-Z]+\b', line.lower()))

    # Filter stop words
    filteredRDD = wordsRDD.filter(lambda word: word not in stop_words and len(word) > 1)

    # Create pair RDD (word, 1)
    pairRDD = filteredRDD.map(lambda w: (w, 1))

    # Count word frequencies
    wordCountRDD = pairRDD.reduceByKey(lambda x, y: x + y)

    # Swap (word, count) -> (count, word)
    swappedRDD = wordCountRDD.map(lambda x: (x[1], x[0]))

    # Sort by frequency (descending)
    sortedRDD = swappedRDD.sortByKey(ascending=False)

    # Collect top 10 results
    top_10 = sortedRDD.take(10)

    print("\nTop 10 most frequent words:")
    for count, word in top_10:
        print(f"   {word}: {count}")

    print(f"\nTotal words (after filtering): {filteredRDD.count()}")
    print(f"Unique words: {wordCountRDD.count()}")

    sc.stop()


if __name__ == "__main__":
    main()
