package spark

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {
  def session: SparkSession
}
