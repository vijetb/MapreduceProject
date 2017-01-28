#
# A simple makefile for compiling three java classes
#


# typing 'make' will invoke the first target entry in the makefile 
# (the default one in this case)
#
default: test

# this target entry builds the Average class
# the Average.class file is dependent on the Average.java file
# and the rule associated with this entry gives the command to create it
#
test: 
	mvn clean install

local:
	java -jar target/Assigment_1-0.0.1-SNAPSHOT.jar 1865.csv