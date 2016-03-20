package test.testSpark

/**
 * @author arunchan05
 */
 
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object avgRainfall {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    val conf = new SparkConf().setAppName("Spark_Rainfall");
    conf.setMaster("local[*]");
    var sc = new SparkContext(conf);
    val iRDD = sc.textFile("data/input/India_rainfall_details.txt");
    val fRDD = iRDD.filter(f=>f.startsWith("Andhra Pradesh"))
    val r1 = fRDD.map(f=>f.split("\t")).map(l=>(l(0),l(1),l(2),(l(3)+l(4))))
    
   // fRDD.saveAsTextFile("data/Output/Spark_Rainfall_"+System.currentTimeMillis());
  }
}