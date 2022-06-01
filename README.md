# Mapreduce Project

### Introduction
A MapReduce program to generate the bag-of-word (BoW) vectors for text files

### Environment
Hadoop 2.8.5 single-node environment on my local machine(MacOS)

### stopwords
private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i", "it", "for", "not", "on", "with", "he", "as", "you", "do", "at", "this", "but", "his", "by", "from", "they", "we", "say", "her", "she", "or", "an", "will", "my", "one", "all", "would", "there", "their", "what", "so", "up", "out", "if", "about", "who", "get", "which", "go", "me", "when", "make", "can", "like", "time", "no", "just", "him", "know", "take", "people", "into", "year", "your", "good", "some", "could", "them", "see", "other", "than", "then", "now", "look", "only", "come", "its", "over", "think", "also", "back", "after", "use", "two", "how", "our", "work", "first", "well", "way", "even", "new", "want", "because", "any", "these", "give", "day", "most", "us" };

### Hadoop operation guide:
./start-all.sh(Use JPS to check the status of all nodes)

If the DataNode cannot start, delete the DFS and format the Namenode

Upload input files to the HDFS directory: hadoop fs -put XXXfile.txt /InputXXX
(Use hadoop fs -ls / to check directory, if not, you need to create a new one)

Run jar on hadoop: hadoop jar XXX.jar XXX /InputXXX /output

Check the output: hadoop fs -cat /output/part-r-00000
(Repeated runs need to delete output: hdfs dfs -rm -r /output)

./stop-all.sh
