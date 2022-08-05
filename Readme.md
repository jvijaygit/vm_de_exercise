# VM Data Engineering Exercise
This repository contains my solution for Data Engineer Tech test.

# <u>Task 1</u>
## Brief
Write an Apache Beam batch job in Python satisfying the following requirements 
1. Read the input from `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`
1. Find all transactions have a `transaction_amount` greater than `20`
1. Exclude all transactions made before the year `2010`
1. Sum the total by `date`
1. Save the output into `output/results.jsonl.gz` and make sure all files in the `output/` directory is git ignored

If the output is in a CSV file, it would have the following format
```
date, total_amount
2011-01-01, 12345.00
...
```

## solution
* solution code is in ```src/task1.py```

# <u>Task 2</u>
## Brief
Following up on the same Apache Beam batch job, also do the following 
1. Group all transform steps into a single `Composite Transform`
2. Add a unit test to the Composite Transform using tooling / libraries provided by Apache Beam

## Solution
* solution code is in ```src/task2.py```
* corresponding unit test is in ```test/task2_test.py```

