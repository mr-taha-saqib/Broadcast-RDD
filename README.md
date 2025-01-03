# PySpark Tasks

This repository contains a series of PySpark tasks for working with RDDs. Below are the tasks and their descriptions.

## Tasks

### 1. Broadcast Join Between Two Pair RDDs
Perform a **broadcast join** between two pair RDDs:
- The first RDD contains key-value pairs: `"id"` and `"name"`.
- The second RDD contains key-value pairs: `"id"` and `"age"`.

Perform a broadcast join on the `"id"` column and return a new RDD, `result`, containing `"id"`, `"name"`, and `"age"`.

---

### 2. Sum of All Values Using an Accumulator
Calculate the **sum of all values** in an RDD using an accumulator:
- Create an RDD of integers from 1 to 10.
- Define an accumulator with an initial value of 0.
- Use the `foreach()` action on the RDD to update the accumulator with each element.
- Finally, print the final value of the accumulator after processing all elements.

---

### 3. Top 5 Key-Value Pairs with Highest Values
Given a text file called `text.txt` with lines in the format `key,value` (where key and value are separated by a comma), write a PySpark code to:
- Read the `text.txt` file.
- Create a Pair RDD where key-value pairs are extracted from the text lines.
- Print the final output, which should be the top 5 key-value pairs with the highest values in descending order.

---

### 4. Top 5 Subjects with the Lowest Names
Write a PySpark code to:
- Read the `scores.txt` file.
- Create a Pair RDD where the key is the subject name and the value is the marks obtained by students.
- Print the final output, which should be the top 5 subjects with the lowest names in ascending order.

---

### 5. Sum of Squares of Even Numbers
Create an RDD of the first 20 integers and print the final output, which should be the sum of the squares of all the even numbers in the list.

---

## Prerequisites
- Python 3.x
- PySpark installed and configured
- Basic understanding of PySpark RDDs and transformations/actions

## Usage
1. Set up your PySpark environment.
2. Use the provided code for each task.
3. Replace the file paths (like `scores.txt` or `text.txt`) with your actual data files.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
