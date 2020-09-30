Assignment-4 Equijoin of two tables using map-reduce program

Mapper Class: Map
# The class will receive data in key and which is stored in string variable. Then this string is splitted by "comma". So here from the data I get the joining condition from second character of string array. I also differentiated between two different datas as their would be no redundant data while joining because I have taken the table's name from the first character in the string array.

Reducer Class: Reduce
#This class will make two different arraylist based on the tables given and then running the for loops in each table I made the join and added that to the string variable. So that way we get the output result called joinresult.

Main class: main
# In this class, basically the inputformatclass and outputformatclass and the path of input file and the output file are given.  