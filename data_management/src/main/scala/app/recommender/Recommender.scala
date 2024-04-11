package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions.asJavaCollection

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext,
                  index: LSHIndex,
                  ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {

  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)

  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Obtain the set of movie IDs that the user has already rated
    val userRatedMovies = ratings.groupBy(_._1).lookup(userId).head.map(_._2)

    // Broadcast the set of user-rated movies to be accessible by all worker nodes
    val broadcastUserRatedMovies = sc.broadcast(userRatedMovies)

    // Find the list of movies that are similar to the given genre
    val similarMovies = nn_lookup.lookup(sc.parallelize(List(genre)))
      .flatMap(_._2) // Flatten the results to obtain movie IDs
      .map(_._1) // Extract the movie ID from the result
      // Filter out movies that the user has already rated
      .filter(movieId => !broadcastUserRatedMovies.value.contains(movieId))
      .collect() // Collect the filtered movie IDs
      .toList // Convert the collection to a list

    // Use the baseline predictor to predict the user's ratings for the similar movies
    val predictedRatings = similarMovies.map(movieId => (movieId, baselinePredictor.predict(userId, movieId)))

    // Sort the predicted ratings in descending order and return the top K recommendations
    predictedRatings.sortBy(_._2).reverse.take(K)
  }


  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    // Obtain the set of movie IDs that the user has already rated
    val userRatedMovies = ratings.groupBy(_._1).lookup(userId).head.map(_._2)

    // Broadcast the set of user-rated movies to be accessible by all worker nodes
    val broadcastUserRatedMovies = sc.broadcast(userRatedMovies)

    // Find the list of movies that are similar to the given genre
    val similarMovies = nn_lookup.lookup(sc.parallelize(List(genre)))
      .flatMap(_._2) // Flatten the results to obtain movie IDs
      .map(_._1) // Extract the movie ID from the result
      // Filter out movies that the user has already rated
      .filter(movieId => !broadcastUserRatedMovies.value.contains(movieId))
      .collect() // Collect the filtered movie IDs
      .toList // Convert the collection to a list

    // Use the collaborative predictor to predict the user's ratings for the similar movies
    val predictedRatings = similarMovies.map(movieId => (movieId, collaborativePredictor.predict(userId, movieId)))

    // Sort the predicted ratings in descending order and return the top K recommendations
    predictedRatings.sortBy(_._2).reverse.take(K)
  }
}
