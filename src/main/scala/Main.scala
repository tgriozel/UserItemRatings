import com.typesafe.config.ConfigFactory
import model.DefaultAggregatorParameters
import spark._

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 1)
      throw new IllegalArgumentException("One (and only) argument expected: input filename")

    val conf = ConfigFactory.load
    val (inputFN, usersFN, itemsFN, ratingsFN) = (
      args(0),
      conf.getString("output.usersFileName"),
      conf.getString("output.itemsFileName"),
      conf.getString("output.ratingsFileName")
    )

    val sparkSession = LocalSparkSessionProvider.session
    val aggregator = new RatingsAggregator(sparkSession, CsvDataFrameWriter, DefaultAggregatorParameters)
    aggregator.writeThreeFiles(inputFN, usersFN, itemsFN, ratingsFN)
    sparkSession.close()
  }

}
