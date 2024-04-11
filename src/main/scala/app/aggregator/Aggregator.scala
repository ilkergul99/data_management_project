package app.aggregator

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var state:RDD[(Int, (Double, Int, String, List[String]))] = null
  private var partitioner: HashPartitioner = null

  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {

    // Transform ratings and titles into key-value pairs with title_id as the key
    val ratingsTransformed = ratings.map {
      case (userId, titleId, oldRating, newRating, timestamp) => (titleId, (userId, oldRating, newRating, timestamp))
    }
    val titlesTransformed = title.map {
      case (titleId, titleName, titleKeywords) => (titleId, (titleName, titleKeywords))
    }

    // Perform a rightOuterJoin on transformed ratings and titles using title_id as the key
    val combinedData = ratingsTransformed.rightOuterJoin(titlesTransformed)

    // Create the aggregatedData RDD by applying the aggregateByKey operation on the combinedData RDD.
    val aggregatedData = combinedData.aggregateByKey((0.0, 0, "", List[String]()))(
      // Sequence operation: (accumulator, value) => updated accumulator
      (accumulator, value) => {
        // value._1 contains the rating data (if it exists) for the current title_id
        value._1 match {
          // If the rating data exists and there's an old rating, update the sum of ratings
          // by adding the new rating and subtracting the old rating
          case Some((_, oldRatingOpt, newRating, _)) if oldRatingOpt.isDefined =>
            val oldRating = oldRatingOpt.get
            (accumulator._1 + newRating - oldRating, accumulator._2, value._2._1, value._2._2)
          // If the rating data exists but there's no old rating, add the new rating
          // to the sum of ratings and increment the rating count
          case Some((_, _, newRating, _)) =>
            (accumulator._1 + newRating, accumulator._2 + 1, value._2._1, value._2._2)
          // If the rating data doesn't exist, keep the accumulator unchanged
          case None =>
            (accumulator._1, accumulator._2, value._2._1, value._2._2)
        }
      },
      // Combiner operation: (accumulator1, accumulator2) => merged accumulator
      (accumulator1, accumulator2) => {
        // Merge accumulators by adding their sums of ratings and rating counts.
        // Keep the title name and title keywords unchanged (from accumulator1).
        (accumulator1._1 + accumulator2._1, accumulator1._2 + accumulator2._2, accumulator1._3, accumulator1._4)
      }
    )

    // Partition the RDD by title_id and persist it in memory
    partitioner = new HashPartitioner(aggregatedData.partitions.length)
    state = aggregatedData.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = {
    state.map { case (_, (ratingSum, ratingCount, titleName, _)) =>
      // Calculate the average rating if the title has been rated
      val averageRating = if (ratingCount > 0) ratingSum / ratingCount else 0.0

      // Return a tuple containing the title name and its average rating
      (titleName, averageRating)
    }
  }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    // Filter the state RDD to only keep titles that contain all the keywords and have been rated
    val ratedTitlesWithKeywords = state.filter {
      case (_, (sumOfRatings, ratingCount, _, titleKeywords)) =>
        keywords.forall(keyword => titleKeywords.contains(keyword)) && ratingCount != 0
    }

    // Count the number of rated titles with the specified keywords
    val ratedTitleCount = ratedTitlesWithKeywords.count()

    // Check if there are any titles with the specified keywords
    if (ratedTitleCount == 0) {
      // If no rated titles have the specified keywords, check if any unrated titles have them
      if (state.filter {
        case (_, (_, _, _, titleKeywords)) => keywords.forall(keyword => titleKeywords.contains(keyword))
      }.isEmpty()) {
        -1.0 // If no titles have the specified keywords, return -1.0
      } else {
        0.0 // If only unrated titles have the specified keywords, return 0.0
      }
    } else {
      // Calculate the sum of the average ratings for each rated title
      val sumOfAverages = ratedTitlesWithKeywords.map {
        case (_, (sumOfRatings, ratingCount, _, _)) => sumOfRatings / ratingCount
      }.reduce(_ + _)

      // Return the average of averages
      sumOfAverages / ratedTitleCount
    }
  }


  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   *  @param delta Delta ratings that haven't been included previously in aggregates
   *        format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Convert the input delta array to an RDD and map it to the format (title_id, (user_id, old_rating, new_rating, timestamp))
    val delta = sc.parallelize(delta_).map {
      case (userId, titleId, oldRating, newRating, timestamp) => (titleId, (userId, oldRating, newRating, timestamp))
    }

    // Aggregate the delta RDD by title_id to combine multiple ratings for the same title into a single entry
    val aggregated = delta.aggregateByKey((0.0, 0))(
      // In the 'seqOp', we process the ratings of a single title in a single partition
      (accumulator, ratingData) => ratingData._2 match {
        // If there is no previous rating, update the sum of ratings and increment the new ratings count
        case None => (accumulator._1 + ratingData._3, accumulator._2 + 1)
        // If there is a previous rating, update the sum of ratings by the difference between the new and old ratings
        case Some(oldRating) => (accumulator._1 + ratingData._3 - oldRating, accumulator._2)
      },
      // In the 'combOp', we combine the results from different partitions for the same title
      (accumulatorPartition1, accumulatorPartition2) => (accumulatorPartition1._1 + accumulatorPartition2._1, accumulatorPartition1._2 + accumulatorPartition2._2)
    )

    // Join the state RDD (containing the current state) with the aggregated delta RDD (containing the new rating information)
    val joined = state.leftOuterJoin(aggregated)

    // Update the state RDD with the new rating data from the aggregated delta RDD
    val updated = joined.mapValues {
      // If there are no new ratings for a title, keep the current data unchanged
      case (currentData, None) => currentData
      // If there are new ratings for a title, update the sum of ratings and rating count with the new information
      case ((sumOfRatings, ratingCount, titleName, titleKeywords), Some((deltaSum, deltaCount))) =>
        (sumOfRatings + deltaSum, ratingCount + deltaCount, titleName, titleKeywords)
    }

    // Unpersist the old state RDD from memory to free up resources, and persist the updated RDD in memory and disk for future use
    state.unpersist()
    state = updated.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }

}
