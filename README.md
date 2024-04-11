# Project Overview

Our project revolves around a database derived from the renowned MovieLens datasets. This database encompasses an array of films and television series, each associated with their respective genres, in conjunction with a comprehensive log of user ratings. The primary objective of our endeavor is to exploit this data repository to augment a video streaming application.

The application, while not directly managed by us, benefits from the data we process and uses it to deliver personalized recommendations and intriguing statistics to its users. Particularly, for each movie or TV series, the application provides a list of similar titles based on keywords, along with the average rating.

In order to ensure the accuracy and efficiency of our solution, we will conduct testing both locally and on GitLab using small and medium-sized datasets. This will enable us to verify the effectiveness of our code in different environments and with different data sizes. Our ultimate aim is to deliver a robust solution that successfully supports the application's needs, providing a better viewing and discovery experience for the end-users.

## MoviesLoader

`MoviesLoader` class is used to load movie data. It accepts a `SparkContext` and a file path as parameters in its constructor.

The `load()` function in this class:
1. Reads the movie file located at the given path and transforms it into an RDD.
2. Cleans the line to remove any unnecessary characters like quotes.
3. Splits the line using the pipe (|) as a separator. This is done assuming that the file is pipe-separated.
4. The first two elements are taken as the movieId and movieTitle respectively. The rest of the elements are taken as genres and stored in a mutable `ArrayBuffer`.
5. The function then returns an RDD consisting of tuples with the movieId, movieTitle, and a list of genres.

The class also includes error handling. If the file is not found at the specified path, it prints out an error message and returns an empty RDD.

## RatingsLoader

`RatingsLoader`, much like `MoviesLoader`, is a class used for loading data. However, this class focuses on loading ratings data.

The `load()` function in this class:
1. Reads the ratings file and transforms it into an RDD.
2. Splits each line using the pipe (|) as a separator.
3. Extracts the userId, movieId, rating, and timestamp from each line and creates an RDD with these fields.
4. Groups the records by the user-movie pair.
5. Sorts the ratings by timestamp and adds an oldRating field. The oldRating field is None for the first rating of each user-movie pair, and for the rest, it takes the value of the previous rating.
6. Returns an RDD containing tuples with userId, movieId, oldRating, rating, and timestamp.

Similar to `MoviesLoader`, error handling is included. If the file is not found at the specified path, it prints an error message and returns an empty RDD.

In both classes, the `persist()` function is used to store the RDD in memory and on disk for better performance.

## SimpleAnalytics

SimpleAnalytics is a package which was designed to perform analysis on movie ratings data. We've implemented various methods to analyze and derive insights from the data. Below is a brief overview of these methods:

### `init(ratings: RDD[(Int, Int, Option[Double], Double, Int)], movie: RDD[(Int, String, List[String])])`

This is an initialization method that prepares the dataset for further processing. It takes two parameters - an RDD of ratings data and an RDD of movies data. The method maps these RDDs to have appropriate keys for subsequent analysis and creates a partitioner for efficient data handling. The processed RDDs are then persisted for quick access in later computations.

### `getNumberOfMoviesRatedEachYear: RDD[(Int, Int)]`

This method is designed to provide yearly statistics on the number of movies rated. It processes the dataset to count the number of movies rated each year and returns an RDD where each element is a tuple. The first element of the tuple is the year, and the second element is the number of movies rated in that year.

### `getMostRatedMovieEachYear: RDD[(Int, String)]`

This method is used to find the most rated movie for each year. It first calculates the count of ratings for each movie per year and sorts them. It then identifies the movie with the most ratings for each year. The result is an RDD containing tuples, where the first element is the year and the second element is the title of the most rated movie for that year. If there are any inconsistencies in the data processing logic leading to duplicate years, an exception is thrown.

### `getMostRatedGenreEachYear: RDD[(Int, List[String])]`

This method finds out the most rated genre of each year. Similar to the previous method, it calculates the count of ratings per movie and identifies the movie with the most ratings for each year. It then fetches the genres of these movies. The result is an RDD of tuples, where the first element is the year and the second element is a list of the most rated genres for that year. If there are any errors causing duplicate years, an exception is thrown.

### `getMostAndLeastRatedGenreAllTime: ((String, Int), (String, Int))`

This method is designed to provide an overall view of genre popularity. It calculates the count of ratings per movie for all years and identifies the genre with the most and least ratings. The result is a tuple containing two pairs. The first pair is the least popular genre and its count, and the second pair is the most popular genre and its count.

### `getAllMoviesByGenre(movies: RDD[(Int, String, List[String])], requiredGenres: RDD[String]): RDD[String]`

This method is used to filter movies based on genres. It takes an RDD of movies and an RDD of required genres as parameters. It filters the movies that have any of the required genres and returns an RDD of the titles of these movies.

### `getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])], requiredGenres: List[String], broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String]`

This method also filters movies based on genres but uses broadcast variables for efficient distribution of data across nodes. It takes an RDD of movies, a list of required genres, and a callback function to broadcast the genres as parameters. It filters the movies that have any of the required genres and returns an RDD of the titles of these movies.

## Aggregator

The Aggregator class computes aggregates from a dataset of movie ratings and titles. It calculates the average rating for each movie and allows querying these aggregates by certain keywords. It also supports incremental updates with new rating data. The class is initialized with a SparkContext, which manages the data and coordinates the computation process in Spark.

### `init(ratings: RDD[(Int, Int, Option[Double], Double, Int)], title: RDD[(Int, String, List[String])])`

This method sets up the state of the aggregator with initial data. The parameters are two RDDs representing ratings and movie titles. The ratings RDD contains tuples of user IDs, movie IDs, possible old ratings, new ratings, and timestamps. The titles RDD contains tuples of movie IDs, movie names, and keyword lists.

First, both RDDs are transformed into key-value pairs with movie IDs as keys. This is because movie ID is the common attribute between both RDDs and it is used to join them later.

Then, a right outer join operation is performed on the transformed RDDs, which combines the ratings and titles based on the movie ID. A right outer join ensures that all movie titles are included in the combined dataset even if they don't have any associated ratings.

The combined dataset is then aggregated by movie ID. For each movie, the aggregate operation calculates the total sum of ratings, counts the number of ratings, and retains the movie name and its associated keywords.

The aggregated data is then partitioned by movie ID for better data locality and performance in Spark. This partitioned data is persisted in memory and on disk for efficient access in subsequent operations.

### `getResult(): RDD[(String, Double)]`

This method calculates the average rating for each movie and returns an RDD of movie names and their average ratings. If a movie has not been rated, its average rating is set to 0.0.

### `getKeywordQueryResult(keywords: List[String]): Double`

This method calculates the average rating for movies that contain all specified keywords.

It first filters the state to keep only the movies that have been rated and contain all the given keywords. It then checks whether there are any such movies.

If no rated movies contain the given keywords, it checks whether there are any unrated movies that contain the keywords. If there are, it returns 0.0, otherwise it returns -1.0.

If there are rated movies that contain the given keywords, it calculates the average of their average ratings and returns this value.

### `updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)])`

This method incrementally updates the state of the aggregator with new rating data. It takes an array of delta ratings, which represents new ratings that haven't been included in the aggregates yet.

The delta ratings array is converted to an RDD and aggregated by movie ID. The aggregate operation updates the sum of ratings and count of ratings for each movie based on the new ratings.

The state of the aggregator is then updated with the new rating data. For each movie, if there are no new ratings, the current data is kept unchanged. If there are new ratings, the sum of ratings and count of ratings are updated.

Finally, the old state is unpersisted to free up memory, and the updated state is persisted for future use.

## LSHIndex

We've implemented the class `LSHIndex`, which is initialized with two inputs: the title data (represented as an RDD of tuples containing title ID, title name, and a list of keywords for each title) and a seed for hashing the keyword lists.

### `hash(input: RDD[List[String]]) : RDD[(IndexedSeq[Int], List[String])]`
We also provide a hash function that computes the hash value of an RDD of keyword lists. This function can be used to hash new queries, preparing them for lookups in the LSH index.

### `getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])]`

The titles are then grouped by their computed signatures into 'buckets'. Each bucket contains all the titles that share the same hash value. The bucketing process helps in organizing the data in a way that similar items are likely to be found in the same bucket. Then, by calling this function user is able to retrieve the buckets RDD for subsequent uses for the LSH index.

### `lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)]): RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])]`

The class also includes a lookup function that takes an RDD of queries (each containing a pre-computed signature and a payload), and returns an RDD of triplets containing the signature, payload, and the result list from the LSH index. If no match is found in the index, an empty result list is returned.

Throughout the LSHIndex process, we've ensured that the 'buckets' RDD is cached for efficient repeated access, improving the overall performance of our program.

## BaselinePredictor

BaselinePredictor is a class that facilitates baseline prediction of movie ratings in a recommender system. This prediction model leverages the average user ratings and average deviations per movie to forecast a user's rating for a particular movie. Below, we outline the methods and components of this class:

### Initialization and Variables

Upon instantiation, the BaselinePredictor class initializes with an empty state and three null RDD variables: `avgUserRating`, `deviationPerRating`, and `avgDeviationPerMovie`. These variables are used later in the class to store intermediate data necessary for making predictions.

### `scale(x: Double, y: Double): Double`

This private method is designed to scale values based on certain conditions. It takes two doubles as input parameters and returns a double value. This method is used in the calculation of rating deviations and the final prediction.

### `init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit`

This method is utilized to initialize the necessary variables for prediction. It receives an RDD of ratings data and performs three key tasks:

1. **Calculates average user ratings:** It groups ratings by user ID, computes the sum and count of ratings for each user, and stores this data in the `avgUserRating` RDD.

2. **Calculates the deviation for each rating:** It calculates a deviation for each user's rating from their average rating, scaling this deviation using the `scale` method. The deviations are then stored in the `deviationPerRating` RDD.

3. **Calculates the average deviation for each movie:** It groups deviations by movie ID, computes the sum and count of deviations for each movie, and stores this data in the `avgDeviationPerMovie` RDD.

All the RDDs created in this method are partitioned appropriately for efficient computation and are cached for quick access in subsequent computations.

### `predict(userId: Int, movieId: Int): Double`

This method is the heart of the BaselinePredictor class, predicting a user's rating for a given movie. It retrieves the average user rating and average deviation for a movie, if available, and applies the following logic:

- If both user rating and movie deviation are unavailable, it returns the global average rating.
- If only the movie deviation is unavailable, it returns the average rating of the user.
- If only the user rating is unavailable, it returns the average deviation of the movie.
- If both are available, it combines the user's average rating and the movie's average deviation to provide a baseline prediction.

## CollaborativeFiltering

CollaborativeFiltering is a class that implements the Collaborative Filtering technique, a common method used in recommendation systems to suggest items based on the behavior of similar users. It leverages the Alternating Least Squares (ALS) algorithm, which is a matrix factorization approach to collaborative filtering. Here is a description of the methods and components of this class:

### Initialization and Variables

When a CollaborativeFiltering instance is created, it requires several parameters: `rank`, `regularizationParameter`, `seed`, and `n_parallel`. These parameters are used later in the ALS training process. Additionally, the `maxIterations` variable is set to 20, and a `model` variable is declared as null. The `model` variable is later used to store the trained ALS model.

### `init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit`

This method prepares and trains the ALS model. It takes an RDD of ratings data and performs the following operations:

1. **Transforms the input data:** The input ratings data is converted into the `Rating` object format that ALS requires. The `Rating` class is a wrapper around the user ID, movie ID, and rating value.

2. **Trains the ALS model:** The ALS model is trained using the transformed input data, the parameters passed during the class instantiation, and the `maxIterations` variable. The trained model is stored in the `model` variable for later use in predictions.

### `predict(userId: Int, movieId: Int): Double`

This method predicts a user's rating for a given movie by leveraging the trained ALS model. It takes a user ID and a movie ID as input and returns a predicted rating value.

## Recommender
The `Recommender` class is used to generate movie recommendations for users. It uses two different prediction techniques: Baseline Prediction and Collaborative Filtering. Let's understand the structure and methods of this class:

### Initialization and Variables

The class is initialized with a SparkContext, an LSHIndex object, and an RDD of ratings data. It uses the LSHIndex to find movies similar to a given genre and the ratings data to train prediction models.

The class has several private variables:

- `nn_lookup` is a nearest neighbor lookup object used to find movies similar to a given genre.
- `collaborativePredictor` is a CollaborativeFiltering object used to predict user ratings based on similar user behavior. It is initialized with certain parameters and trained with the ratings data.
- `baselinePredictor` is a BaselinePredictor object used to predict user ratings based on overall average ratings and individual user and movie biases. It is also trained with the ratings data.

### `recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)]`

This method generates recommendations using the baseline predictor. It takes a user ID, a list of genres, and a number K as input and returns a list of K movie IDs and their predicted ratings. Here's the process:

1. It finds the movies that the user has already rated.
2. It broadcasts the user-rated movies to all worker nodes.
3. It finds movies similar to the given genre.
4. It filters out the movies that the user has already rated.
5. It predicts the user's ratings for the remaining movies using the baseline predictor.
6. It sorts the predicted ratings in descending order and returns the top K movie IDs and their predicted ratings.

### `recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)]`

This method is similar to `recommendBaseline`, but it uses the collaborative predictor instead of the baseline predictor to predict user ratings. 

