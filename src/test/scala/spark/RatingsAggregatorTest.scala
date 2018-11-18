package spark

import java.time.ZonedDateTime

import model.DefaultAggregatorParameters
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.mockito.Mockito._
import org.scalatest.FlatSpec
import org.scalatest.Matchers._
import org.scalatest.mockito.MockitoSugar

class RatingsAggregatorTest extends FlatSpec with MockitoSugar {

  private def timestampMinusHours(diff: Int) = refTimestamp - (diff * 1000 * 60 * 60)

  private val p = DefaultAggregatorParameters
  private val writer = mock[DataFrameWriter]
  private val session = LocalSparkSessionProvider.session
  import session.implicits._

  private val refTimestamp = ZonedDateTime.now().toEpochSecond * 1000
  private val baseData = Seq[(String, String, Float, Long)](
    ("user1", "item1", 1.0f, timestampMinusHours(0)),
    ("user1", "item2", 0.9f, timestampMinusHours(6)),
    ("user1", "item2", 0.9f, timestampMinusHours(230)),
    ("user2", "item1", 0.8f, timestampMinusHours(35)),
    ("user2", "item1", 0.7f, timestampMinusHours(100)),
    ("user3", "item1", 1.0f, timestampMinusHours(30))
  )
  private val baseDf = baseData.toDF(p.userIdColName, p.itemIdColName, p.ratingColName, p.timestampColName)

  behavior of "lookupDataFrame"

  it should "create a DataFrame with only the raw Ids and generated ones (consecutive Ids starting at 0)" in {
    // Given
    val aggregator = new RatingsAggregator(session, writer, p)
    // When
    val result = aggregator.lookupDataFrame(baseDf, p.userIdColName, StringType, p.userIdIntColName)
    // Then
    result.schema should be(StructType(Array(
      StructField(p.userIdColName, StringType, nullable = false),
      StructField(p.userIdIntColName, LongType, nullable = false)
    )))
    val content = result.collect()
    content.map(_.getAs[String](0)) should contain theSameElementsAs Seq("user1", "user2", "user3")
    content.map(_.getAs[Long](1)) should contain theSameElementsAs Seq(0, 1, 2)
  }

  behavior of "aggregatedRatingsDataFrame"

  it should "produce right values and remove ones below the threshold" in {
    // Given
    val aggregator = new RatingsAggregator(session, writer, p)
    // When
    val result = aggregator.aggregatedRatingsDataFrame(baseDf, refTimestamp)
    // Then
    result.schema should be(StructType(Array(
      StructField(p.userIdColName, StringType),
      StructField(p.itemIdColName, StringType),
      StructField(p.ratingSumColName, DoubleType)
    )))
    val content = result.collect()
    content should contain theSameElementsAs Seq(
      Row("user1", "item1", 1.0),
      Row("user1", "item2", 1.4672244298839734),
      Row("user2", "item1", 1.3301543766152113),
      Row("user3", "item1", 0.95)
    )
  }

  behavior of "generatedIdsAndRatingsDataFrame"

  it should "do the right joins and keep only the interesting columns" in {
    // Given
    val aggregator = new RatingsAggregator(session, writer, p)
    val (usersDf, itemsDf, ratingsDf) = (
      Seq(("user1", 1), ("user2", 2))
        .toDF(p.userIdColName, p.userIdIntColName),
      Seq(("item1", 1), ("item2", 2))
        .toDF(p.itemIdColName, p.itemIdIntColName),
      Seq(("user1", "item1", 1.1), ("user1", "item2", 1.2), ("user2", "item2", 2.2))
        .toDF(p.userIdColName, p.itemIdColName, p.ratingSumColName)
    )
    // When
    val result = aggregator.generatedIdsAndRatingsDataFrame(usersDf, itemsDf, ratingsDf)
    // Then
    result.schema should be(StructType(Array(
      StructField(p.userIdIntColName, IntegerType, nullable = false),
      StructField(p.itemIdIntColName, IntegerType, nullable = false),
      StructField(p.ratingSumColName, DoubleType, nullable = false)
    )))
    val content = result.collect()
    content should contain theSameElementsAs Seq(
      Row(1, 1, 1.1),
      Row(1, 2, 1.2),
      Row(2, 2, 2.2)
    )
  }

  behavior of "buildThreeDataFrames"

  it should "assemble things in the expected way and return the right result" in {
    // Given
    val inputPath = "whatever.csv"
    val aggregator = spy(new RatingsAggregator(session, writer, p))
    val (usersDf, itemsDf, ratingsDf, joinedDf) = (mock[DataFrame], mock[DataFrame], mock[DataFrame], mock[DataFrame])
    doReturn(baseDf, null).when(aggregator).dataFrameFromFilePath(inputPath)
    doReturn(usersDf, null).when(aggregator).lookupDataFrame(baseDf, p.userIdColName, StringType, p.userIdIntColName)
    doReturn(itemsDf, null).when(aggregator).lookupDataFrame(baseDf, p.itemIdColName, StringType, p.itemIdIntColName)
    doReturn(ratingsDf, null).when(aggregator).aggregatedRatingsDataFrame(baseDf, refTimestamp)
    doReturn(joinedDf, null).when(aggregator).generatedIdsAndRatingsDataFrame(usersDf, itemsDf, ratingsDf)
    // When
    val result = aggregator.buildThreeDataFrames(inputPath) 
    // Then
    result should be((usersDf, itemsDf, joinedDf))
  }

  behavior of "writeThreeFiles"

  it should "write the results of 'buildThreeDataFrames'" in {
    // Given
    val (inputFN, usersFN, itemsFN, ratingsFN) = ("input", "users", "items", "ratings")
    val (usersDf, itemsDf, ratingsDf) = (mock[DataFrame], mock[DataFrame], mock[DataFrame])
    val dataFrameWriter = writer
    val aggregator = spy(new RatingsAggregator(session, dataFrameWriter, p))
    doReturn((usersDf, itemsDf, ratingsDf), null).when(aggregator).buildThreeDataFrames(inputFN)
    // When
    aggregator.writeThreeFiles(inputFN, usersFN, itemsFN, ratingsFN)
    // Then
    verify(aggregator).buildThreeDataFrames(inputFN)
    verify(dataFrameWriter).write(usersDf, usersFN)
    verify(dataFrameWriter).write(itemsDf, itemsFN)
    verify(dataFrameWriter).write(ratingsDf, ratingsFN)
  }

}
