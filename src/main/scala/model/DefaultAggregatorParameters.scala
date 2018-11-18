package model

object DefaultAggregatorParameters extends AggregatorParameters(
  minRatingThreshold = 0.1,
  multPenaltyPerDay = 0.95,
  separator = ",",
  userIdColName = "userId",
  itemIdColName = "itemId",
  ratingColName = "rating",
  timestampColName = "timestamp",
  userIdIntColName = "userIdAsInteger",
  itemIdIntColName = "itemIdAsInteger",
  ratingSumColName = "ratingSum"
)
