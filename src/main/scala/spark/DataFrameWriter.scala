package spark

import org.apache.spark.sql.DataFrame

trait DataFrameWriter {
  def write(df: DataFrame, path: String): Unit
}
