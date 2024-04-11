package app.loaders

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ArrayBuffer
import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    // Getting the absolute path of the input file
    val absolutePath = new File(getClass.getResource(path).getFile).getPath
    // Reading the file content using Spark context
    val rawFileContent = sc.textFile(absolutePath)

    try {
      // Processing the raw file content into an RDD of movie data
      val moviesDataRDD = rawFileContent.map(line => {
        // Removing any quotes from the line and splitting it into elements
        val cleanedLine = line.replace("\"", "")
        val elements = cleanedLine.split("\\|")

        // The first element is the movie ID, and the second is the movie title
        val movieId = elements(0).toInt
        val movieTitle = elements(1)

        // The rest of the elements are genres, so we collect them in a list
        val genreList = ArrayBuffer[String]()
        for (i <- 2 until elements.length) {
          genreList += elements(i)
        }

        // Returning a tuple of movie ID, movie title, and list of genres
        (movieId, movieTitle, genreList.toList)
      })

      // Persisting the RDD in memory and disk for faster access in future actions
      moviesDataRDD.persist(StorageLevel.MEMORY_AND_DISK)

      moviesDataRDD
    } catch {
      case e: Exception =>
        println("Error while loading file from " + path + ". Error: " + e)
        // Returning an empty RDD in case of error
        sc.emptyRDD
    }
  }
}

