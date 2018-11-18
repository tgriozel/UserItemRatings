package spark

import model.AggregatorParameters
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class RatingsAggregator(spark: SparkSession, writer: DataFrameWriter, p: AggregatorParameters) extends Serializable {

  val schema = StructType(Array(
    StructField(p.userIdColName, StringType, nullable = false),
    StructField(p.itemIdColName, StringType, nullable = false),
    StructField(p.ratingColName, FloatType, nullable = false),
    StructField(p.timestampColName, LongType, nullable = false)
  ))

  /*
   * Transforms a list of strings to a Row, given that it respects the expected format
   */
  private def fromLineToRow(line: List[String]): Row = line match {
    case u :: i :: r :: t :: Nil => Row(u, i, r.toFloat, t.toLong)
    case _ => throw new IllegalArgumentException(s"Line format is not what is expected")
  }

  /*
   * A simple method to create a DataFrame from the file which path is given as a parameter
   */
  def dataFrameFromFilePath(filePath: String): DataFrame = {
    val rows = spark.sparkContext.textFile(filePath).map(_.split(p.separator).toList).map(fromLineToRow)
    spark.createDataFrame(rows, schema)
  }

  /*
   * Returns the maximum value of the 'timestamp' column contained in the input Data Frame
   */
  def getMaxTimestamp(input: DataFrame): Long = {
    val tempColName =  "referenceTimestamp"
    input
      .agg(max(p.timestampColName).as(tempColName))
      .select(tempColName)
      .collect().headOption.getOrElse(Row(lit(0l)))
      .getAs[Long](0)
  }

  /*
   * Returns a DataFrame containing a mapping of raw Ids to consecutive integer Ids, starting from 0
   */
  def lookupDataFrame(input: DataFrame, fromColName: String, fromColType: DataType, toColName: String): DataFrame = {
    val rdd = input.select(col(fromColName)).distinct().rdd.zipWithIndex().map { case (row, id) => Row(row.get(0), id) }
    spark.createDataFrame(rdd, StructType(Array(
      StructField(fromColName, fromColType, nullable = false),
      StructField(toColName, LongType, nullable = false)
    )))
  }

  /*
   * Returns a DataFrame with a computed rating for each userId/itemId pair (only those 3 rows are returned)
   * The calculation is done as follows:
   * Each individual rating will receive a multiplicative penalty of 'multPenaltyPerDay' for each full day separating
   * the corresponding timestamp from the reference timestamp.
   * The sum of all ratings for the userId/itemId pairs are then calculated.
   * Values inferior to 'minRatingThreshold' are filtered out
   */
  def aggregatedRatingsDataFrame(input: DataFrame, referenceTimestamp: Long): DataFrame = {
    val millisecondsInADay = 1000 * 60 * 60 * 24
    val (multFactorColName, weightedRatingColName) = ("multFactor", "weightedRating")
    input
      .withColumn(
        multFactorColName,
        pow(
          lit(p.multPenaltyPerDay),
          floor(lit(referenceTimestamp).minus(col(p.timestampColName)).divide(lit(millisecondsInADay)))
        )
      )
      .withColumn(
        weightedRatingColName,
        col(p.ratingColName).multiply(col(multFactorColName))
      )
      .groupBy(col(p.userIdColName), col(p.itemIdColName))
      .agg(sum(col(weightedRatingColName)).as(p.ratingSumColName))
      .where(col(p.ratingSumColName).>(lit(p.minRatingThreshold)))
      .select(col(p.userIdColName), col(p.itemIdColName), col(p.ratingSumColName))
  }

  /*
   * Joins raw Ids to generated Ids with the DataFrame containing ratings, keeping only generated Ids and their rating
   */
  def generatedIdsAndRatingsDataFrame(userIds: DataFrame, itemIds: DataFrame, ratings: DataFrame): DataFrame = {
    ratings
      .join(userIds, ratings(p.userIdColName) === userIds(p.userIdColName))
      .join(itemIds, ratings(p.itemIdColName) === itemIds(p.itemIdColName))
      .select(p.userIdIntColName, p.itemIdIntColName, p.ratingSumColName)
  }

  /*
   * Build 3 DataFrames from the data contained in the file designated by 'inputPath'
   * The input csv file is supposed to be split in 4 columns: userId, itemId, rating and timestamp.
   * The 1st DataFrame will contain only the distinct raw userIds with their corresponding integer id
   * The 2nd DataFrame will contain only the distinct raw itemIds with their corresponding integer id
   * The 3rd DataFrame will contain the aggregated ratings for each userId/itemId (the integer versions)
   * The ratings are aggregated and summed in a way explained in the 'aggregatedRatingsDataFrame' method above
   */
  def buildThreeDataFrames(inputPath: String): (DataFrame, DataFrame, DataFrame) = {
    val baseDf = dataFrameFromFilePath(inputPath)
    baseDf.persist() // Let's cache this intensively-used DataFrame
    val usersDf = lookupDataFrame(baseDf, p.userIdColName, StringType, p.userIdIntColName)
    usersDf.persist() // Let's cache this intensively-used DataFrame
    val itemsDf = lookupDataFrame(baseDf, p.itemIdColName, StringType, p.itemIdIntColName)
    itemsDf.persist() // Let's cache this intensively-used DataFrame
    val ratingsDf = aggregatedRatingsDataFrame(baseDf, getMaxTimestamp(baseDf))
    val generatedIdsRatingsDf = generatedIdsAndRatingsDataFrame(usersDf, itemsDf, ratingsDf)
    (usersDf, itemsDf, generatedIdsRatingsDf)
  }

  /*
   * Write the csv files from the DataFrames computed with the 'buildThreeDataFrames' method
   */
  def writeThreeFiles(inputPath: String, usersPath: String, itemsPath: String, ratingsPath: String): Unit = {
    val (usersDf, itemsDf, ratingsDf) = buildThreeDataFrames(inputPath)
    writer.write(usersDf, usersPath)
    writer.write(itemsDf, itemsPath)
    writer.write(ratingsDf, ratingsPath)
  }

}
