package spark

import org.apache.spark.sql.SparkSession

object LocalSparkSessionProvider extends SparkSessionProvider {

  override val session: SparkSession = SparkSession.builder()
    .appName("user-item-ratings")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

}
