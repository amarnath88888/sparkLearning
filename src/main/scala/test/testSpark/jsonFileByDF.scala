package test.testSpark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object jsonFileByDF {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:/Users/Thambu/Desktop/arun ws/testSpark");
    
    val conf = new SparkConf().setAppName("jsonFileByDF").setMaster("local[4]");
    var sc = new SparkContext(conf);
    val sqlContext = new SQLContext(sc);
    // Create the DataFrame
    val df = sqlContext.read.json("data/input/sample.json")
    //df.show();
    
 /*   +--------------------+-----------+--------+-----+--------------------+
      |                 _id|         bp|deviceID|pulse|                  ts|
      +--------------------+-----------+--------+-----+--------------------+
      |[56d7afb6919cb195...| [1.0, 2.0]|     1.0|  1.0|[2016-03-03T03:30...|*/
    
   // df.printSchema();
    /**
              root
       |-- _id: struct (nullable = true)
       |    |-- $oid: string (nullable = true)
       |-- bp: array (nullable = true)
       |    |-- element: double (containsNull = true)
       |-- deviceID: double (nullable = true)
       |-- pulse: double (nullable = true)
       |-- ts: struct (nullable = true)
       |    |-- $date: string (nullable = true)
           */
    
   val df1 = df.filter(df("deviceID") === 2.0 /*&& df("pulse") === 1.0*/)
   df1.filter(df("pulse") === 1.0).show()
  }
  
}