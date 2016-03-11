package test.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CountByExample {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    val conf = new SparkConf().setAppName("CountByExample");
    conf.setMaster("local");
    var sc = new SparkContext(conf);
    val inputRDD = sc.textFile("data/input/empdata.txt");
    val countRDD = inputRDD.map(t => t.split("\t")).map(l=>(l(1)));
    val count = countRDD.countByValue();
    println(count);
  }
}