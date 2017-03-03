# Hadoop-Exercises
1. Russian voting data. 
Using Hadoop compute the results described in a and b. In each case the output needs to be placed in an output file written by either the map or reduce function. The input and output directories need to be command line arguments to the command to run your program as in the WordCount sample programs. That is the input directory needs to be the first argument and the output directory needs to be the second argument. The result of part a should go into a subdirectory of the output directory called “a” and the output of part b goes in a subdirectory of the input directory called “b”. The program needs to be able to run on AWS EMR. This means that each time the program is run multiple copies of both map and reduce may be used.
a. Compute the number of voters in each voting district in Russia. There are 99 districts numbered from 1 to 99.
b. Compute the mean number of voters in voting districts in Russia.


2. WordCount.OnthedatasetpageofcoursewebsiteyouwillfindalinktoMarkTwain’s collected works which will be the input for our word count program. As with problem one your programs should have two command line arguments, the first is the input directory and the second is the output directory. In processing the words you should normalize the words by:
 • Removing the endings “'” (single quote), “--”, “-”, “'s”, “ly”, “ed”, “ing”, “ness”, “)“, “_”, “;”, “?”, “!”, “,”, “:”.
 • Convert all words to lower case
 • Remove leading “‘“ (single quote), “”” (double quote), “(“ and “_”.

a. Producethestandardwordcountbuttheoutputneedstobesortedbythenumberof times the word occurs in decreasing order. The final output should be in a single file even if we use multiple reducers.


b. The second word program counts the occurrence of unordered word pairs.Eachtime two words occur next to each other we count that as a pair. For example in the sentence a cat a rat a bat” we have “a cat” occurs twice, “a rat” occurs twice, “a bat” occurs once as we do not consider the order of the words. The last word in a line is considered as occurring before the first word in the next line. A few word pairs will not be counted if the file processed by multiple map instances. As in part a) we want the output sorted by the count in decreasing order.



   3. 2012USTax Data. This is a csv file. This dataset contains information about income taxes filed in 2012. The data is categorized by state, zipcode and income level. There are six different income levels numbered 1 through 6. So each row in the file contains information about income taxes filed in a given state, in a given zipcode area and in one of 6 income levels. Here is a sample.
 The second column STATE give the abbreviation for the state. The third column indicates the zipcode. The fourth column, AGI_STUB, gives the adjusted gross income given in numbers 1-6 with the meaning listed below.
 
 1 - $1 to $25,000
 2 - $25,000 to $50,000
 3 - $50,000 to $75,000
 4 - $75,000 tor $100,000 5 - $100,000 to $200,000 6 - $200,000 or more



The fourth column, N1, givens the number of returns in the income level from the indicated zipcode. So there were 1600 tax returns from zip code 35004 with income between $1 and $25,000.
a. Write a Hadoop program to find the total number of tax returns filed in each statei n each category. Again the program needs two command line argument, the first the input directory and the second the output directory. The input will be a file as described above. The output will be a file(s) sorted by state. Each state will have the six levels of income with the total of tax returns in each category.
