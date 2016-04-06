hadoop = hadoop

bases = LeftoverMapper LeftoverReducer NodeInputFormat Node NodeOrDouble NodeOutputFormat NodeRecordReader NodeRecordWriter TrustMapper TrustReducer PageRank
classDir = classes
sourceDir = src
HADOOP_DIR=/opt/hadoop-2.7.2
javaFiles = src/*.java
classFiles = classes/*.class

CLASS_PATH=$(HADOOP_DIR)/share/hadoop/common/*:$(HADOOP_DIR)/share/hadoop/yarn/lib/*:$(HADOOP_DIR)/share/hadoop/mapreduce/lib/*:$(HADOOP_DIR)/share/hadoop/mapreduce/*:./
j = -classpath $(CLASS_PATH) #Xlint:deprecation


default : $(javaFiles)
	mkdir $(classDir); javac $j -d $(classDir) $(javaFiles); jar cvf PageRank.jar $(classDir);echo $(classFiles); $(hadoop) jar PageRank.jar PageRank

clean : 
	rm -r $(classDir); rm -r stage*; rm PageRank.jar;



