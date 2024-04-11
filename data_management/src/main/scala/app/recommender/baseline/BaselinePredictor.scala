package app.recommender.baseline

import org.apache.spark.rdd.RDD

class BaselinePredictor() extends Serializable {

  private var state = null
  // Declare the necessary variables

  private var avgUserRating: RDD[(Int, (Double, Int))] = null
  private var deviationPerRating: RDD[((Int, Int), Double)] = null
  private var avgDeviationPerMovie: RDD[(Int, (Double, Int))] = null

  // Implement the scale function
  private def scale(x: Double, y: Double): Double = {
    // If 'x' is greater than 'y', return the result of 5 minus 'y'
    if (x > y) 5 - y
    // If 'x' is less than 'y', return the result of 'y' minus 1
    else if (x < y) y - 1
    // If 'x' is neither greater than nor less than 'y' (i.e., 'x' equals 'y'), return 1
    else 1
  }
  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {

    // Calculate the number of unique users in the dataset by mapping to user IDs, removing duplicates, and counting
    val uniqueUserCount = ratingsRDD.map { case (userId, _, _, _, _) => userId }.distinct().count()

    // Calculate the number of unique movies in the dataset in the same way
    val uniqueMovieCount = ratingsRDD.map { case (_, movieId, _, _, _) => movieId }.distinct().count()

    // Calculate the average rating for each user
    avgUserRating = ratingsRDD
      .groupBy { case (userId, _, _, _, _) => userId } // Group the ratings by user ID
      .map { case (userId, userRatings) =>
        // Calculate the sum of ratings and the number of ratings for each user
        val (totalRatings, ratingCount) = userRatings.foldLeft((0.0, 0)) { case ((accumulatedSum, accumulatedCount), (_, _, _, rating, _)) =>
          (accumulatedSum + rating, accumulatedCount + 1)
        }
        (userId, (totalRatings, ratingCount)) // Return a tuple of user ID, total ratings, and rating count
      }
      .partitionBy(new org.apache.spark.HashPartitioner(uniqueUserCount.toInt)) // Partition the RDD based on the number of unique users
      .cache() // Cache the RDD for faster access in future

    // Calculate the deviation for each rating
    deviationPerRating = ratingsRDD
      .map { case (userId, movieId, _, rating, _) => (userId, (movieId, rating)) } // Map to a tuple of user ID, movie ID, and rating
      .join(avgUserRating) // Join with the average user ratings
      .map { case (userId, ((movieId, rating), (totalRatings, ratingCount))) =>
        // Calculate the user's average rating and the deviation for the current rating
        val userAvgRating = totalRatings / ratingCount
        val ratingDeviation = (rating - userAvgRating) / scale(rating, userAvgRating)
        ((userId, movieId), ratingDeviation) // Return a tuple of user ID, movie ID, and rating deviation
      }
      .partitionBy(new org.apache.spark.HashPartitioner(ratingsRDD.partitions.length)) // Partition the RDD based on the number of partitions in the original ratings RDD
      .cache() // Cache the RDD for faster access in future

    // Calculate the average deviation for each movie
    avgDeviationPerMovie = deviationPerRating
      .groupBy { case ((_, movieId), _) => movieId } // Group the deviations by movie ID
      .map { case (movieId, movieDeviations) =>
        // Calculate the sum of deviations and the number of deviations for each movie
        val (totalDeviations, deviationCount) = movieDeviations.foldLeft((0.0, 0)) { case ((accumulatedSum, accumulatedCount), ((_, _), deviation)) =>
          (accumulatedSum + deviation, accumulatedCount + 1)
        }
        (movieId, (totalDeviations, deviationCount)) // Return a tuple of movie ID, total deviations, and deviation count
      }
      .partitionBy(new org.apache.spark.HashPartitioner(uniqueMovieCount.toInt)) // Partition the RDD based on the number of unique movies
      .cache() // Cache the RDD for faster access in future
  }

  def predict(userId: Int, movieId: Int): Double = {

    // Use the 'lookup' function on 'avgUserRating' to find the average rating for the given user ID
    // 'headOption' is used to retrieve the first element of the returned sequence, or None if the sequence is empty
    val userAvgRatingOpt = avgUserRating.lookup(userId).headOption

    // Similarly, retrieve the average deviation for the given movie ID
    val movieAvgDeviationOpt = avgDeviationPerMovie.lookup(movieId).headOption

    // Use pattern matching to determine the available data and compute the prediction accordingly
    (userAvgRatingOpt, movieAvgDeviationOpt) match {
      case (None, None) =>
        // If no data is available for either the user rating or the movie deviation, calculate the global average rating
        // This is done by summing all ratings and dividing by the total count of ratings
        val globalRatingStats = avgUserRating
          .map { case (_, (sumRatings, count)) => (sumRatings, count) }
          .reduce((ratingStats1, ratingStats2) => (ratingStats1._1 + ratingStats2._1, ratingStats1._2 + ratingStats2._2))
        globalRatingStats._1 / globalRatingStats._2

      case (Some((userTotalRatings, userRatingCount)), None) =>
        // If data is available for the user rating but not for the movie deviation, return the average rating of the user
        userTotalRatings / userRatingCount

      case (None, Some((movieTotalDeviations, movieDeviationCount))) =>
        // If data is available for the movie deviation but not for the user rating, return the average deviation of the movie
        movieTotalDeviations / movieDeviationCount

      case (Some((userTotalRatings, userRatingCount)), Some((movieTotalDeviations, movieDeviationCount))) =>
        // If data is available for both the user rating and the movie deviation, calculate a combined baseline prediction
        // This is done by calculating the user's average rating and the movie's average deviation,
        // then adding the movie's average deviation (scaled by the 'scale' function) to the user's average rating
        val userAvgRating = userTotalRatings / userRatingCount
        val movieAvgDeviation = movieTotalDeviations / movieDeviationCount
        userAvgRating + movieAvgDeviation * scale(userAvgRating + movieAvgDeviation, userAvgRating)
    }
  }
}
