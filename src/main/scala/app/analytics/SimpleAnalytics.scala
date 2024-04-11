package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {
  // Define variables to store partitioners
  private var ratingsPartitioner: HashPartitioner = null
  private var moviesPartitioner: HashPartitioner = null

  // Define variables to store processed RDDs
  private var idGroupedTitles: RDD[(Int, (String, List[String]))] = null
  private var yearTitleGroupedRatings: RDD[(Int, Map[Int, Iterable[(Int, Int, Option[Double], Double)]])] = null

  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    // Map movies RDD to have movieId as the key and a tuple of title and genres as the value
    val idMappedMovies = movie.map(x => {
      // x._1 is the movie ID
      // x._2 is the movie title
      // x._3 is the list of genres
      (x._1, (x._2, x._3))
    })

    // Map ratings RDD to have year as the key and a tuple containing userId, movieId, rating, and timestamp as the value
    val yearMappedRatings = ratings.map(x => {
      // x._1 is the user ID
      // x._2 is the movie ID
      // x._3 is the rating
      // x._4 is the timestamp
      // We need to convert the timestamp to a year, so we multiply it by 1000 (since it's in seconds)
      // and DateTime class to extract the year
      val year = new DateTime(x._5.toLong * 1000).getYear

      (year, (x._1, x._2, x._3, x._4))
    })

    // Group ratings by year
    val yearGroupedRatings = yearMappedRatings.groupByKey()

    // Group ratings by year and movieId
    val yearAndTitleGroupedRatings = yearGroupedRatings.map(x => (x._1, x._2.groupBy(y => y._2)))

    // Create partitioners for processed RDDs
    ratingsPartitioner = new HashPartitioner(yearAndTitleGroupedRatings.getNumPartitions)
    moviesPartitioner = new HashPartitioner(idMappedMovies.getNumPartitions)

    // Partition and persist processed RDDs
    idGroupedTitles = idMappedMovies.partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)
    yearTitleGroupedRatings = yearAndTitleGroupedRatings.partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)

  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = {
    // Map each element of the yearTitleGroupedRatings RDD
    yearTitleGroupedRatings.map(x => {
      // x._1 represents the year (the key in the RDD)
      val year = x._1
      // x._2 represents the value of the RDD, which is a Map with movieId as the key
      // and Iterable of tuples (userId, movieId, rating, timestamp) as the value.
      // By calculating the size of the Map, we can determine the number of movies rated in that year
      val numberOfMoviesRated = x._2.size

      // Return a tuple with the year and the number of movies rated in that year
      (year, numberOfMoviesRated)
    })
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {
    // Transform the yearTitleGroupedRatings to get the count of ratings for each movie in each year
    val yearAndMovieRatingsCounts = yearTitleGroupedRatings.map { case (year, movieRatings) =>
      (year, movieRatings.map { case (movieId, ratings) => (movieId, ratings.size) })
    }

    // For each year, sort the movie-rating counts by the number of ratings first, then by the movieId, both in descending order
    val yearAndSortedMovieRatingsCounts = yearAndMovieRatingsCounts.map { case (year, movieRatingsCounts) =>
      (year, movieRatingsCounts.toList.sortWith { case ((movieId1, count1), (movieId2, count2)) =>
        count1 > count2 || (count1 == count2 && movieId1 > movieId2)
      })
    }

    // For each year, get the movieId of the movie with the most ratings (which is the first element of the list)
    val mostRatedMovieIdAndYear = yearAndSortedMovieRatingsCounts.map { case (year, sortedMovieRatingsCounts) =>
      (sortedMovieRatingsCounts.head._1, year) // format: (movieId, year)
    }

    // Join mostRatedMovieIdAndYear with idGroupedTitles to get the name of the most rated movie,
    // and map the result to an RDD with year as the key and movie name as the value
    val yearAndMostRatedMovieName = mostRatedMovieIdAndYear.join(idGroupedTitles).map { case (movieId, (year, (title, _))) =>
      (year, title) // format: (year, movieTitle)
    }

    yearAndMostRatedMovieName // return the RDD
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {
    // Transform the yearTitleGroupedRatings to get the count of ratings for each movie in each year
    val yearAndMovieRatingsCounts = yearTitleGroupedRatings.map { case (year, movieRatings) =>
      (year, movieRatings.map { case (movieId, ratings) => (movieId, ratings.size) })
    }

    // For each year, sort the movie-rating counts by the number of ratings first, then by the movieId, both in descending order
    val yearAndSortedMovieRatingsCounts = yearAndMovieRatingsCounts.map { case (year, movieRatingsCounts) =>
      (year, movieRatingsCounts.toList.sortWith { case ((movieId1, count1), (movieId2, count2)) =>
        count1 > count2 || (count1 == count2 && movieId1 > movieId2)
      })
    }

    // For each year, get the movieId of the movie with the most ratings (which is the first element of the list)
    val mostRatedMovieIdAndYear = yearAndSortedMovieRatingsCounts.map { case (year, sortedMovieRatingsCounts) =>
      (sortedMovieRatingsCounts.head._1, year) // format: (movieId, year)
    }

    // Join mostRatedMovieIdAndYear with idGroupedTitles to get the genres of the most rated movie,
    // and map the result to an RDD with year as the key and genre list as the value
    val yearAndMostRatedGenres = mostRatedMovieIdAndYear.join(idGroupedTitles).map { case (movieId, (year, (_, genres))) =>
      (year, genres) // format: (year, List[Genre])
    }

    yearAndMostRatedGenres // return the RDD
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int)) = {
    // Map yearTitleGroupedRatings RDD to get the count of ratings for each movie
    val mostRatedMovie = yearTitleGroupedRatings.map(x => (x._1, x._2.map(y => (y._1, y._2.size))))

    // Sort mostRatedMovie by the count of ratings in descending order and then by movie ID in ascending order
    val mostRatedMovieSorted = mostRatedMovie
      .map(x => (x._1, x._2.toList.sortWith((a, b) =>
        a._2 > b._2 || (a._2 == b._2 && a._1 > b._1)
      )))

    // Get the most rated movie ID for each year
    val mostRatedMovieId = mostRatedMovieSorted.map(x => (x._2.head._1, x._1))

    // Join mostRatedMovieId with idGroupedTitles RDD to get the genres for each most rated movie
    val mostRatedMovieGenre = mostRatedMovieId.join(idGroupedTitles).map(x => (x._2._1, x._2._2._2))

    // Flatten mostRatedMovieGenre by genres and count of ratings for each genre
    val mostRatedMovieGenreFlattened = mostRatedMovieGenre.flatMap(x => x._2.map(y => (y, x._1)))

    // Count the number of movies for each genre
    val mostRatedMovieGenreCount = mostRatedMovieGenreFlattened.countByKey()

    // Sort the genres by count of movies in descending order and then by name in ascending order
    var mostRatedMovieGenreCountSorted = mostRatedMovieGenreCount.toSeq.sortWith((x, y) => {
      if (x._2 == y._2) x._1 < y._1
      else x._2 > y._2
    })

    // Get the most popular genre
    val mostPopularGenre = mostRatedMovieGenreCountSorted.head

    // Sort the genres by count of movies in ascending order and then by name in ascending order
    mostRatedMovieGenreCountSorted = mostRatedMovieGenreCountSorted.sortWith((x, y) => {
      if (x._2 == y._2) x._1 < y._1
      else x._2 < y._2
    })

    // Get the least popular genre
    val leastPopularGenre = mostRatedMovieGenreCountSorted.head

    // Return a tuple of least popular genre and count, and most popular genre and count
    ((leastPopularGenre._1, leastPopularGenre._2.toInt), (mostPopularGenre._1, mostPopularGenre._2.toInt))
  }


  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    // Collect required genres into a Set
    val requiredGenresSet = requiredGenres.collect().toSet

    // Filter the movies RDD by checking if any genre of the movie is present in the requiredGenresSet
    val filteredMovies = movies.filter { case (_, _, genres) => genres.exists(requiredGenresSet.contains) }

    // Map the filtered RDD to keep only the movie titles
    val movieTitles = filteredMovies.map { case (_, title, _) => title }

    // Return the RDD with movie titles
    movieTitles
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    // Broadcast the requiredGenres list to all Spark executors using the broadcastCallback function
    val broadcastedGenres = broadcastCallback(requiredGenres)

    // Filter the movies RDD by checking if any genre of the movie is present in the broadcastedGenres.value
    val filteredMovies = movies.filter { case (_, _, genres) => genres.exists(broadcastedGenres.value.contains) }

    // Map the filtered RDD to keep only the movie titles
    val movieTitles = filteredMovies.map { case (_, title, _) => title }

    // Return the RDD with movie titles
    movieTitles
  }

}

