# Data Pipeline

## ATHENA
- read Commoncrawl's index and filter to job websites only
- export to CSV in S3: `athena/outputCSV/*.csv` in the bucket `maria-batch-1005`

## PYTHON (pyspark and boto3)
- read all CSVs from `athena/outputCSV/` in S3 (Pyspark does this natively)
- for each CSV, parse it into rows containing pointers (S3 object and offset/length in the object) to where to get the original WARC file (Pyspark)
- for each row, actually retrieve the WARC file from the public Commoncrawl location (boto3)
- parse the WARC response to get the WARC body, which is the entirety of the HTML code (warcio)
- TODO: save HTML code to S3 or else otherwise be able to pass it to scala

## SCALA (Spark)
- TODO: read in the HTML code from S3 or however else we're passing the HTML code
- TODO: actually run our analyses
- 

# Environment Setup
- Have Python 3 installed (I'm using 3.8.5, which came default with WSL)
- Have Pip (Python's package manager) installed: `sudo apt install python3-pip`
- Install all Python packages: run `pip install -r requirements.txt`
	- for some reason Pip takes like 5-10 minutes to start up in WSL. I don't know why. The actual running will take a few minutes as well
- Set up AWS access. You'll need to add two .jar files to your $SPARK_HOME/jars directory (download them from the internet and put them there). Courtesy of Duncan:
```
aws-java-sdk-X.X.X.jar  ----->	HAVE TO GET COMPATIBLE VERSION TO '$SPARK_HOME/jars/hadoop-common-X.X.X.jar'
                                I have hadoop-common-2.7.4.jar: aws sdk needed was 1.7.4 from:
                                https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk/1.7.4
                                did a chmod 644 to match other jar permissions

hadoop-aws-X.X.X.jar  -------> 	HAVE TO GET SAME VERSION AS '$SPARK_HOME/jars/hadoop-common-X.X.X.jar'
                                I have hadoop-common-2.7.4.jar: got the jar from:
                                https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/2.7.4
                                did a chmod 644 to match other jar permissions
```
- Also, you'll need to have two environment variables containing your credentials. In your .bashrc (and obviously replace the xxxxxxx with the actual credentials found in Gio's pinned post in #project-3-general - and then also remember to either restart your terminal or run `source ~/.bashrc` to actually have the exports take effect):
```
export AWS_ACCESS_KEY_ID=xxxxxxx
export AWS_SECRET_ACCESS_KEY=xxxxxxxx
```
- Everything is now set up. You should be able to run `python3 fetch_job_pages.py` (or `python` instead of `python3` depending on how it's set up on your computer)

# Keys Package
- keys is a part of the git ignore but you will need to add the package yourself when cloning the project.
- under src/main/scala create a new package called 'keys' within that package make a new Scala Object also called 'keys'
- within that object at the following:
```
package keys

object keys {
  final val AccessKey = "replace_with_key"
  final val SecretKey = "replace_with_key"
}
```
