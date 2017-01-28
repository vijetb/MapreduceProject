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
	mvn -f PagerankSpark/ clean install	
	# replace the "in" with the path to the input files folder, "input" should be the output directory path
	java -jar PageRankSpark/target/PageRankSpark-0.0.1-SNAPSHOT-jar-with-dependencies.jar wikipedia-simple-html.bz2 output