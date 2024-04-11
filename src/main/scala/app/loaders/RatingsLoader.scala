package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  // Function to load the data, process it and return an RDD with (userId, movieId, oldRating, rating, timestamp) tuples
  def load(): RDD[(Int, Int, Option[Double], Double, Int)] = {
    // Retrieve the absolute path of the input file
    val absoluteFilePath = new File(getClass.getResource(path).getFile).getPath
    // Load the raw file content using Spark context
    val rawReviewsContent = sc.textFile(absoluteFilePath)

    try {
      // Map each line of raw reviews content into a tuple (userId, movieId, rating, timestamp)
      val initialReviewData = rawReviewsContent.map(line => {
        val dataElements = line.split("\\|")
        val userId = dataElements(0).toInt
        val movieId = dataElements(1).toInt
        val rating = dataElements(2).toDouble
        val timestamp = dataElements(3).toInt
        (userId, movieId, rating, timestamp)
      })

      // Group the reviews by user-movie pair
      val groupedReviewData = initialReviewData.groupBy(x => (x._1, x._2))

      // Add previous rating to each review and create new tuples
      val finalReviewData = groupedReviewData.flatMap { case ((userId, movieId), listRatings) =>
        // Sort the ratings by timestamp
        val sortedRatings = listRatings.toList.sortBy(_._4)
        // Add previous rating to each review and create new tuples
        sortedRatings.zipWithIndex.map { case ((_, _, currentRating, timestamp), index) =>
          val previousRating = if (index == 0) None else Some(sortedRatings(index - 1)._3)
          (userId, movieId, previousRating, currentRating, timestamp)
        }
      }.persist(StorageLevel.MEMORY_AND_DISK) // Persist the RDD in memory and disk for efficient access

      finalReviewData // Return the final RDD
    } catch {
      // Handle any exceptions that might occur during the process
      case exception: Exception =>
        println("Error while loading file from " + path + ". Error: " + exception)
        // Return an empty RDD in case of an error
        sc.emptyRDD
    }
  }
}