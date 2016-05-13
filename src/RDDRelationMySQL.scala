import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object RDDRelationMySQL {
  def main(args: Array[String]) {
    val url = "jdbc:mysql://localhost:3306/presto?user=prestouser&password=password"

    val sparkConf = new SparkConf().setAppName("RDDRelation").setMaster("local")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    // Importing the SQL context gives access to all the SQL functions and implicit conversions.
    import sqlContext.implicits._

    val restaurants = sqlContext.jdbc(url, "restaurant")
    val dishes = sqlContext.jdbc(url, "dish")
    val users = sqlContext.jdbc(url, "user")
    val friends = sqlContext.jdbc(url, "friend")
    val tags = sqlContext.jdbc(url, "tag")
    val likes = sqlContext.jdbc(url, "activity_log").filter("activity_type = 11")
    
    println ("restaurants RDD size: " + restaurants.collectAsList().size())
    println ("dish RDD size: " + dishes.collectAsList().size())
    println ("users RDD size: " + users.collectAsList().size())
    println ("friends RDD size: " + friends.collectAsList().size())
    println ("tags RDD size: " + tags.collectAsList().size())
    println ("likes RDD size: " + likes.collectAsList().size())

    sc.stop()
  }
}
