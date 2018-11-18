package spark

import org.apache.spark.sql.DataFrame

object CsvDataFrameWriter extends Serializable with DataFrameWriter {

  override def write(df: DataFrame, path: String): Unit = {
    df.write.csv(path)
  }

}
