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

### TDL

#### MVPs:
	
* Entry Level Experience
	* What percentage of entry level tech jobs require previous experience?
* Largest Job Seekers
	* What are the three companies posting the most tech job ads
* Tech Job Posting Trends
	* Is there a general trend in tech job postings over the past year?
	* What about the past month?
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

## Usage

* /path to project folder/ sbt package 
* Upload .jar file into bucket on S3
* On EMR add step
	* Type: Spark Application
	* Deploy Mode: Cluster
	* Submit Options: ---class project3.Runner
	* Application Location: .jar file in S3 bucket

## Contributors

Revature Reston 210405 Big Data batch (April  2021- June 2021)
