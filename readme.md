# Programming Exercise using PySpark

## Software:
macOS: Monterey 12.6

IDE: PyCharm 2021.3.2 (Professional Edition)

Python 3.7.15

PySpark 3.3.1

Scala 2.12.5

pyenv 2.3.5

## Explanations:
1. I used spark session to initiate the environment
2. I loaded the data sets
3. I removed the column email from the first data set
4. I created a list of the countries which I had to filter on
5. I filtered on the data set on UK and NL
6. I removed the credit card numbers from the second data set
7. I renamed the following columns:
**id** into **client_identifier** (in the second data set)
**btc_a** into **btc_address**
**cc_t** into **credit_card_type**
8. I created a new data frame which is an inner join of the two data sets I was given initially
9. I exported the newly created data frame as a csv file into the folder called **client_data**
10. I committed everything into a GitHub repository
