package app.recommender.collaborativeFiltering


import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
class CollaborativeFiltering(rank: Int,
                             regularizationParameter: Double,
                             seed: Long,
                             n_parallel: Int) extends Serializable {

  // NOTE: set the parameters according to the project description to get reproducible (deterministic) results.
  private val maxIterations = 20
  private var model:MatrixFactorizationModel = null

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    // Transform the input data into the format required by ALS (Rating objects)
    val alsInputData = ratingsRDD.map { case (userId, movieId, _, rating, _) => Rating(userId, movieId, rating) }

    // Initialize and train the ALS model
    model = ALS.train(alsInputData, rank, maxIterations, regularizationParameter, n_parallel, seed)
  }

  def predict(userId: Int, movieId: Int): Double = {
    // Use the model to predict the rating for the given user and movie
    model.predict(userId, movieId)
  }

}
