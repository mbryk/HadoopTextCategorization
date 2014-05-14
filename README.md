HadoopTextCategotization
====

Distributed Text Categorization

## Description ##

This project classifies the category of a piece of text, given
sufficient training data.

This project was created for ECE465 Cloud Computing at Cooper Union,
taught by Professor Rob Marano in the Spring 2014 term.

## Dependencies ##

* Java 7
* Hadoop 2.x.x
* Maven 2

## Usage ##

### General Syntax ###
java Main.class [-f|--featurize training-file-dir output-dir] [-t|--trained trained-file-dir testing-dir output-dir labels-file] [training-file-dir testing-dir output-dir labels-file]

To run, use mvn compile exec:exec while setting the appropriate arguments in the maven pom.xml file. Examples of possible arguments are commented out in the pom file.

### Options ###

-f|--featurize Only run the word count mapreduce job on the training articles. Takes the hdfs directory of training articles as well as an hdfs directory to put the featurized output

-t|--trained Run the KNN algorithm on already featurized training data. Takes the hdfs directory of already featurized training data, a local directory of testing articles, an hdfs output directory, as well as a local file of testing article labels

Without any flags, both mapreduce jobs for featurizing and KNN are run taking the hdfs directory of training data, a local directory of testing articles, an hdfs output directory, as well as a local file of testing article labels
