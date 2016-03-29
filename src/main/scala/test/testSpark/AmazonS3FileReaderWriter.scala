package test.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * @author arunchan05
 *
 */
object AmazonS3FileReaderWriter {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    val conf = new SparkConf().setAppName("AmazonS3FileReaderWriter");
    conf.setMaster("local[*]");
    var sc = new SparkContext(conf);
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "*******")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "*********")
    val myRDD = sc.textFile("s3n://hospitaldevice/grades.json")
  val filRDD =  myRDD.map(m=>m.contains("50906d7fa3c412bb040eb57b"))
   filRDD.saveAsTextFile("s3n://hospitaldevice/output2/")
    //println(myRDD.count)
  }
}