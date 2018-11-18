package model

case class AggregatorParameters(
  minRatingThreshold: Double,
  multPenaltyPerDay: Double,
  separator: String,
  userIdColName: String,
  itemIdColName: String,
  ratingColName: String,
  timestampColName: String,
  userIdIntColName: String,
  itemIdIntColName: String,
  ratingSumColName: String
)
