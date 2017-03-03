1) Question 1
These commands are used to compile and run. i installed hadoop on local machine (Mac) using homebrew
I have used two mapper and reducer and performed two jobs

Note: i have set the number of reduce to 1 

File name: RussElection.java
Step1 : create a directory ex: russia
Step2 : compile -> $ javac -classpath hadoop-core-1.2.1.jar -d russia  RussElection.java
Step3 : create a jar file -> $ jar -cvf Russia.jar -C russia/ .
Step4 : Run -> hadoop jar Russia.jar RussElection input_dir output

To view output 
 $ hadoop fs -ls -R
 $ hadoop fs -cat output/a/part-*
 $ hadoop fs -cat output/b/part-*

 2) Question 2
2a)
File name:Wordcounter.java
Step1 : create a directory ex:wordcounter
Step2 : compile -> javac -classpath hadoop-core-1.2.1.jar -d wordcounter  Wordcounter.java
Step3 : create a jar file -> $ jar -cvf wordcounter.jar -C wordcounter/ .
Step4 : Run ->  hadoop jar wordcounter.jar Wordcounter input_dir2 out14


2b) 
File name: WordOccurence.java
Step1 : create a directory ex:wordoc
Step2 : compile -> javac -classpath hadoop-core-1.2.1.jar -d wordoc WordOccurence.java
Step3 : create a jar file -> $ jar -cvf wordoc.jar -C wordoc/ .
Step4 : Run ->  hadoop jar wordoc.jar WordOccurence input_dir2 out15

 3) Question 3
 I have used CompositeGroupKey

 File name: USTax.java
 Step1 : create a directory ex: tax
 Step2 : compile -> $ javac -classpath hadoop-core-1.2.1.jar -d tax USTax.java
 Step3 : create a jar file -> jar -cvf tax.jar -C tax/ .
 Step4 : Run -> hadoop jar tax.jar USTax input_dir1 output1

 To view output
 $ hadoop fs -cat output1/part-*

 To merge 
 hdfs -merge src dest

Refference :
Stackoverflow,github,
for regex 