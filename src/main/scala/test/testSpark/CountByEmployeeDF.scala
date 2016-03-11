package test.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

case class Emp(id: Int, country: String, gender: String, salary: Int)

object  CountByEmployeeDF {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    
    val conf = new SparkConf().setAppName("CountByEmployeeDF");
    conf.setMaster("local[4]");
    var sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    import sqlContext.implicits._
    val emp = sc.textFile("data/input/empdata.txt").map(_.split("\t")).map(e => Emp(e(0).toInt, e(1), e(2), e(3).toInt)).toDF();
    emp.registerTempTable("employee");
    val filteredEmployees = sqlContext.sql("SELECT id, country, gender, salary FROM employee WHERE country = 'india' AND salary <= 50 and gender = 'm'")
    filteredEmployees.collect()
    filteredEmployees.collect().foreach(println);
    
  }
}