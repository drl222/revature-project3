# Revature Project 3
## Project Description

> Process job listing data obtained from Amazon S3 as part of the Amazon Public Datasets program (https://commoncrawl.org/the-data/get-started/). Determining the following information utilizing the Spark ecosystem supported by Hadoop resources:
* The percentage of entry level tech jobs that require previous experience.
* Any significant spikes in tech job postings at the end of business quarters.
* Top three companies posting tech job ads.
* Any general trend in tech job postings over the past year/month.
* Percentage of tech job posters post no more than three job ads a month.
* Graphical Display of Data for Presentation


## Technologies Used

* AWS
	* Athena
	* S3
	* EMR
* Spark ecosystem
	* Spark SQL
	* Dataframes, Datasets, RDDs
	* Scala
* SBT
* RegEx
* Zeppelin
* Hadoop and related resources

## Features

### Complete

#### MVPs:
* Extract testing data
  	* Create framework to access data
* Job Posting Spikes
	* Is there a significant spike in tech job postings at the end of business quarters?
	* If so, which quarter spikes the most?
* Entry Level Experience
	* What percentage of entry level tech jobs require previous experience?
* Largest Job Seekers
	* What are the three companies posting the most tech job ads
* Graphical Display of Data for Presentation

### Stretch:
* Tech Job Posting Trends
	* Is there a general trend in tech job postings over the past year?
	* What about the past month?
* Percent of Relatively Infrequent Job Seekers
	* What percent of tech job posters post no more than three job ads a month?

### To-Do List

#### MVPs:
	
* Entry Level Experience
	* Make Regex more Robust to capture different HTML Structures over the years. 
	* Reevaluate performance cost of using WET Files instead of WARC Files to scrape for experience.
	* Expand to other sites like Monster and Glassdoor, depending on performance cost of WET vs regex WARC.
* Graphical Display of Data for Presentation
	* Establish Zeppelin on a cluster connected to a primary table to allow more efficient visualization of a wider array of data for all project members

## Getting Started

* Clone the project: `gh repo clone drl222/revature-project3`
* Configure the following technologies with versions:
	* AWS EMR Cluster: 5.33.0
	* JDK: 1.8.x
	* Scala: 2.11.12
	* Hadoop: 2.10.1
	* Spark: 2.4.7
	* Zeppelin: 0.9.0
	* SBT: 1.5.3
* For supplemental Python script, install Python 3 and pip, and run `pip install -r requirements.txt`

## Usage

* /path to project folder/ sbt package 
* Upload .jar file into bucket on S3
* On EMR add step
	* Type: Spark Application
	* Deploy Mode: Cluster
	* Submit Options: --class project3.Runner
	* Application Location: .jar file in S3 bucket
* The results are written back to S3, in the locations given by the code
* Supplemental Python script is for manual investigation of HTML files. Run it like any other Python file, `python fetch_job_pages.py`. You'll need Spark running locally before it can run.

## Contributors

Revature Reston 210405 Big Data batch (April  2021- June 2021)

## Presentation
![image](https://user-images.githubusercontent.com/82099912/122787020-87231b00-d27a-11eb-88e3-9d9f1431a419.png)
![image](https://user-images.githubusercontent.com/82099912/122787097-9a35eb00-d27a-11eb-8a64-c242a58766b7.png)
![image](https://user-images.githubusercontent.com/82099912/122787269-c81b2f80-d27a-11eb-9079-4de0bbb865a1.png)
![image](https://user-images.githubusercontent.com/82099912/122787302-d0736a80-d27a-11eb-9078-6a60e0525914.png)
![image](https://user-images.githubusercontent.com/82099912/122787359-df5a1d00-d27a-11eb-9ec4-3bcebee60a02.png)
![image](https://user-images.githubusercontent.com/82099912/122787409-ec770c00-d27a-11eb-9b52-83a310635e47.png)
![image](https://user-images.githubusercontent.com/82099912/122787449-f6990a80-d27a-11eb-8337-c099547811ce.png)
![image](https://user-images.githubusercontent.com/82099912/122787488-00227280-d27b-11eb-9c35-ac46c9adf879.png)
![image](https://user-images.githubusercontent.com/82099912/122787542-0d3f6180-d27b-11eb-8fe6-ca453dcdd0ea.png)
![image](https://user-images.githubusercontent.com/82099912/122787574-15979c80-d27b-11eb-90ad-6d793c5fcbee.png)
![image](https://user-images.githubusercontent.com/82099912/122787614-1f210480-d27b-11eb-8d8b-d104f98cca9a.png)
![image](https://user-images.githubusercontent.com/82099912/122787647-27793f80-d27b-11eb-91dc-a3b287beb20f.png)
![image](https://user-images.githubusercontent.com/82099912/122787682-306a1100-d27b-11eb-85be-dd9dd24728cc.png)
![image](https://user-images.githubusercontent.com/82099912/122787734-3cee6980-d27b-11eb-9117-7ee6f00fc1ae.png)
![image](https://user-images.githubusercontent.com/82099912/122787775-45df3b00-d27b-11eb-85bf-a490ef6d4f04.png)
![image](https://user-images.githubusercontent.com/82099912/122787799-4ed00c80-d27b-11eb-8269-76207406096e.png)
![image](https://user-images.githubusercontent.com/82099912/122787827-55f71a80-d27b-11eb-9e6c-9a5cc206deda.png)
![image](https://user-images.githubusercontent.com/82099912/122787910-6d360800-d27b-11eb-9ee3-624140e57a28.png)
![image](https://user-images.githubusercontent.com/82099912/122787948-758e4300-d27b-11eb-98c7-0db408cfd898.png)
![image](https://user-images.githubusercontent.com/82099912/122787984-7c1cba80-d27b-11eb-90ca-d744e1e2e8f1.png)
![image](https://user-images.githubusercontent.com/82099912/122788019-83dc5f00-d27b-11eb-8db9-75d3120edd0a.png)

