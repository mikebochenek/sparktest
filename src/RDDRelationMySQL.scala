import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object RDDRelationMySQL {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val url = "jdbc:mysql://localhost:3306/presto?user=prestouser&password=password"
    val restaurants = sqlContext.jdbc(url, "restaurant")
    
    println ("restaurants RDD size: " + restaurants.collectAsList().size())
    
    

    sc.stop()
  }
}
