package test.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType};


object ProcessHealthData {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    
    val conf = new SparkConf().setAppName("ProcessHealthData").setMaster("local[4]");
    var sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    
    val df = sqlContext.read.json("data/Output/healthData.json");
    //df.show()
    df.registerTempTable("JSON");
    
    df.printSchema();
    sqlContext.sql("""SELECT max(value) FROM JSON where unit = "count"""").show()

  }
}