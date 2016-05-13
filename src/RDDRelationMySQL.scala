import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.slf4j.Logger
import org.slf4j.LoggerFactory;

object RDDRelationMySQL {
  def main(args: Array[String]) {
    val startTS = System.currentTimeMillis
    val logger = LoggerFactory.getLogger("RDDRelationMySQL");
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
    
    logger.info ("restaurants RDD size: " + restaurants.collectAsList().size())
    logger.info ("dish RDD size: " + dishes.collectAsList().size())
    logger.info ("users RDD size: " + users.collectAsList().size())
    logger.info ("friends RDD size: " + friends.collectAsList().size())
    logger.info ("tags RDD size: " + tags.collectAsList().size())
    logger.info ("likes RDD size: " + likes.collectAsList().size())

    dishes.foreach { x => logger.info(x.toString) }
    
    logger.info ("... all done in: " + (System.currentTimeMillis - startTS) + "ms")
    sc.stop()
  }
}
