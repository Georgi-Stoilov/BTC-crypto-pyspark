# Programming Exercise using PySpark

## Software:
macOS: Monterey 12.6

IDE: PyCharm 2021.3.2 (Professional Edition)

Python 3.7.15

PySpark 3.3.1

Scala 2.12.5

pyenv 2.3.5

## Explanations main.py:
1. I used spark session to initiate the environment
2. I have setup logging in PySpark
3. I loaded the data sets
4. I removed the column email from the first data set
5. I created a list of the countries which I had to filter on
6. I filtered on the data set on UK and NL
7. I removed the credit card numbers from the second data set
8. I renamed the following columns:
**id** into **client_identifier** (in the second data set)
**btc_a** into **btc_address**
**cc_t** into **credit_card_type**
9. I created a new data frame which is an inner join of the two data sets I was given initially
10. I exported the newly created data frame as a csv file into the folder called **client_data**
11. I committed everything into a GitHub repository


## Explanations main-solution.py:
I created this file to test and debug my code.
It is not necessary to read it.