#
# A simple makefile for compiling three java classes
#


# typing 'make' will invoke the first target entry in the makefile 
# (the default one in this case)
#
default: test

# this target entry builds the Average cla/Users/Viji/Desktop/MR_1/test/makefiless
# the Average.class file is dependent on the Average.java file
# and the rule associated with this entry gives the command to create it
#
test: 
	mvn -f Assignment-2-No-Combiner/ clean install
	mvn -f Assignment-2-With-Combiner/ clean install
	mvn -f Assignment-2-With-InMapper-Combiner/ clean install
	mvn -f Assignment-2a/ clean install

mapreduce-with-no-combiner:
	java -jar Assignment-2-No-Combiner/target/Assignment-2-0.0.1-SNAPSHOT-jar-with-dependencies.jar Assignment-2-No-Combiner/1763.csv no-combiner-output
mapreduce-with-combiner:
	java -jar Assignment-2-With-Combiner/target/Mapreduce-With-Combiner-Counter-0.0.1-SNAPSHOT-jar-with-dependencies.jar Assignment-2-With-Combiner/1763.csv with-combiner-output
mapreduce-with-in-mapper-combiner:
	java -jar Assignment-2-With-InMapper-Combiner/target/MapReduceWithInMapperCombiner-0.0.1-SNAPSHOT-jar-with-dependencies.jar Assignment-2-With-InMapper-Combiner/1763.csv inmapper-combiner-output
mapreduce-with-secondary-sort:
	java -jar Assignment-2a/target/Secondary-Sort-0.0.1-SNAPSHOT-jar-with-dependencies.jar Assignment-2a/1763.csv secondary-sort-output
