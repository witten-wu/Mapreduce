# Mapreduce_Demo
### Hadoop operation guide:
./start-all.sh(Use JPS to check the status of all nodes)

If the DataNode cannot start, delete the DFS and format the Namenode

Upload input files to the HDFS directory: hadoop fs -put XXXfile.txt /InputXXX
(Use hadoop fs -ls / to check directory, if not, you need to create a new one)

Run jar on hadoop: hadoop jar XXX.jar XXX /InputXXX /output

Check the output: hadoop fs -cat /output/part-r-00000
(Repeated runs need to delete output: hdfs dfs -rm -r /output)

./stop-all.sh
