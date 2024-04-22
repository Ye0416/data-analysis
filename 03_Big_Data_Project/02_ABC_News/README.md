# ABC News
Detecting popular and trending topics from the news articles is an important task for  public opinion monitoring. In this project, your task is to perform text data analysis over a dataset of Australian news from ABC (Australian Broadcasting Corporation) using both RDD and DataFrame APIs of Spark with Python. The problem is to compute the weights of each term regarding each year in the news articles dataset and then select the top-k most important terms in each year. 

### Input files: 

The dataset you are going to use contains data of news headlines published over several years. In this text file, each line is a headline of a news article, in format of "date,term1 term2 ... ... ". The date and texts are separated by a comma, and the terms are separated by the space character. A sample file is like below: 
​		20030219,council chief executive fails to secure position 
​		20030219,council welcomes ambulance levy decision 
​		20030219,council welcomes insurance breakthrough 
​		20030219,fed opp to re introduce national insurance 
​		20040501,cowboys survive eels comeback 
​		20040501,cowboys withstand eels fightback 
​		20040502,castro vows cuban socialism to survive bush 
​		20200401,coronanomics things learnt about how coronavirus economy 
​		20200401,coronavirus at home test kits selling in the chinese community 
​		20200401,coronavirus campbell remess streams bear making classes 
​		20201015,coronavirus pacific economy foriegn aid china 
​		20201016,china builds pig apartment blocks to guard against swine flu 

### Term weights computation: 

You need to ignore the stop words such as “to”, “the”, and “in”. There is also a stop word list stored in "stopword.txt"

To compute the weight for a term regarding a year, please use the TF/IDF model. 
Specifically, the TF and IDF can be computed as: 

TF(term t, year y) = the number of headlines containing t in y 

IDF(term t, dataset D) = log10 (the number of years in D/the number of years having t) 

Finally, the term weight of term t regarding the year y is computed as: Weight(term t, year y, dataset D) = TF(term t, year y)* IDF(term t, dataset D) 

Please import math and use math.log10() to compute the term weights, and round the results to 6 decimal places. 



### Output format: 

If there are N years in the dataset, you should output exactly N lines in your final output file, and these lines are sorted by years in ascending order. In each line, you need to output a list of k pairs in format of , and these pairs are sorted by term weights in descending order. If two terms have the same weight, sort them alphabetically. Specifically, the format of each line is like: “year\t Term1,Weight1;Term 2,Weight2;… …;Termk,Weightk”. For example, given the above data set and k=3, the output should be: 
​		2003 council,1.431364;insurance,0.954243;welcomes,0.954243 
​		2004 cowboys,0.954243;eels,0.954243;survive,0.954243 
​		2020 coronavirus,1.908485;china,0.954243;economy,0.954243 
