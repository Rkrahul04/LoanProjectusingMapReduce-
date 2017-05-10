# LoanProjectusingMapReduce-

Problem Statement:  
Here, we have chosen the loan dataset on which we have performed map-reduce operations. We hproblems to fetch the data. In that, we have explained only 2 over here. Rest you can try at your enand solution in given in next section. 
1. Finding the list of people with particular grade who have taken loan.
1. Finding the list of people with having interest more than certain value like 1000.
1. Finding the list of people with having loan amount more than certain value.
1. Get maximum number of loan given to which grade users (A-G).
1. Highest loan amount given in that year with that Employee id and Employees annual
   income.
1. Get the total number of loans with loan id and load amount which are all having loa
   status as Late?
1. Average loan interest rate with 60 month term and 36 month term. 

## Tools and Technologies used: 
 
1. Hadoop Environment 
1. Eclipse

## Dataflow Diagram:
![Diagram](https://github.com/Rkrahul04/blog/blob/master/loan_1.jpg)

## Implementation:

Here, we are solving the problems in map-reduce programming with the help of eclipse. For this, we 
have imported the complete project in our workspace of eclipse and configured all the JARs which
are present under lib folder. 

*Command: Hadoop dfs –copyFromLocal loan.csv hdfs:/*
*Command: Hadoop jar loan1.jar /loan.csv /loan_output*

![digram-2](https://github.com/Rkrahul04/blog/blob/master/loan_2.jpg)

Here, we have created the output folder loan_output, where we are storing the output.

![digram-3](https://github.com/Rkrahul04/blog/blob/master/loan_3.jpg)

## Output:
![digram-4](https://github.com/Rkrahul04/blog/blob/master/loan_4.jpg)

*Problem 2: Finding the list of people with having interest more than certain value like 
1000.* 
We have moved the data to HDFS, then execute the Hadoop command on dataset.

*Command: Hadoop dfs –copyFromLocal loan.csv hdfs:/*
*Command: Hadoop jar loan2.jar /loan.csv /loan_output2*
![digram-5](https://github.com/Rkrahul04/blog/blob/master/loan_5.jpg)

Here, we have created the output folder loan_output2, where we are storing the output.

*Code Explanation:*

In mapper:
We have first filtered the data and read 3 columns by using split function which are: 
1. Id
1. Loan_amount
1. Int_rate 

In reducer:
We have converted the integer parts to float values and have taken the interest using the formula of 
simple interest. 

*Output:*

![digram-6](https://github.com/Rkrahul04/blog/blob/master/loan_6.jpg)



