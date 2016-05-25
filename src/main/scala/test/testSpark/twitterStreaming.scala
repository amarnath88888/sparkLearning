/**
 *
 */
package test.testSpark

import scala.collection.mutable.HashMap
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.twitter.TwitterUtils
/**
 * @author arunchan05
 *
 */
object twitterStreaming {
  def main(args: Array[String]){
    val apiKey = "***";
    val apiSecret = "####";
    val accessToken = "%%%%%%%";
    val accessTokenSecret = "$$$$$$";
    configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret);
    
    val sparkConf = new SparkConf().setAppName("TwitterPopularTagsTest").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(1));
    val tweets = TwitterUtils.createStream(ssc, None);
    //tweets.print();
    val statuses = tweets.map(status => status.getText());
    //val words = statuses.flatMap(status => status.split(" "))
    val hashtags = statuses.filter(word => word.contains("#"))
    val counts = hashtags.map(tag => (tag, 1)).reduceByKey(_ + _)
    hashtags.print();
    ssc.start()
    ssc.awaitTermination()
    
  }
  
/** Configures the Oauth Credentials for accessing Twitter */
  def configureTwitterCredentials(apiKey: String, apiSecret: String, accessToken: String, accessTokenSecret: String) {
    val configs = new HashMap[String, String] ++= Seq(
      "apiKey" -> apiKey, "apiSecret" -> apiSecret, "accessToken" -> accessToken, "accessTokenSecret" -> accessTokenSecret)
    println("Configuring Twitter OAuth")
    configs.foreach{ case(key, value) => 
        if (value.trim.isEmpty) {
          throw new Exception("Error setting authentication - value for " + key + " not set")
        }
        val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
        System.setProperty(fullKey, value.trim)
        println("\tProperty " + fullKey + " set as [" + value.trim + "]") 
    }
    println()
  }

}
