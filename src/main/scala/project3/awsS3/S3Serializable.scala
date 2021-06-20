package project3.awsS3

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import keys.keys


import java.nio.charset.{Charset, CodingErrorAction}
import java.util.zip.GZIPInputStream


//to make it Serializable for the Worker Nodes when in Cluster Distributive Environment by making it static
//https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
object S3Serializable {
  val s3Creds = new BasicAWSCredentials(keys.AccessKey, keys.SecretKey)
  val s3Client =
    AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(s3Creds))
      .withRegion("us-east-1")
      .build()

  /**
   * getContentFromS3Object utilizes the Amazon S3 Java API to specifically traverse the Common Crawl Index by
   * using the withRange function of S3Objects to skip and point to the correct Warc Page content (using file offset and length)
   * within the accumulated file (@param filename) that contains multiple warc pages.
   *
   * @param filename   = the filename path excluding the root bucket
   * @param fileOffset = the respective file offset for fileName
   * @param fileLength = the respective file length for fileName
   * @return = a String of the entire content of the fileName
   */
  def getContentFromS3Object(filename: String, fileOffset: Long, fileLength: Long): String = {
    //https://stackoverflow.com/questions/24537884/how-to-get-first-n-bytes-of-file-from-s3-url
    //https://stackoverflow.com/questions/13625024/how-to-read-a-text-file-with-mixed-encodings-in-scala-or-java
    val s3Client = S3Serializable.s3Client
    //make a request to get the file from commoncrawl
    val request = new GetObjectRequest("commoncrawl", filename)
    //set the specific page within the warc file
    request.withRange(fileOffset, fileOffset + fileLength - 1)
    //retrieve it
    val s3WarcObject = s3Client.getObject(request)
    //check for any malformed input in the input stream
    val decoder = Charset.forName("UTF-8").newDecoder
    decoder.onMalformedInput(CodingErrorAction.IGNORE)
    //use GZIP input stream to unzip the .gz so we can use fromInputStream to convert to String with mkString
    val warcContent: String =
      scala.io.Source.fromInputStream(
        new GZIPInputStream(s3WarcObject.getObjectContent())
      ).mkString
    //release the resources
    s3WarcObject.close()

    warcContent
  }
}
