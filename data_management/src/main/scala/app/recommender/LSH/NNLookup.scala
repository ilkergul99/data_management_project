package app.recommender.LSH

import org.apache.spark.rdd.RDD

/**
 * Class for performing LSH lookups
 *
 * @param lshIndex A constructed LSH index
 */
class NNLookup(lshIndex: LSHIndex) extends Serializable {

  /**
   * Lookup operation for queries
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (keyword list, resut) pairs
   */
  def lookup(queries: RDD[List[String]])
  : RDD[(List[String], List[(Int, String, List[String])])] = {
    // First, compute the hash of each query in the input RDD using the provided
    // hash function. This will create an RDD of (signature, keyword list) pairs.
    val hashedQueries = lshIndex.hash(queries)

    // Next, perform the lookup operation using the LSH index. This requires passing
    // the hashedQueries RDD to the lookup function of the LSHIndex class. The result
    // will be an RDD of (signature, keyword list, result) triplets.
    val lookupResults = lshIndex.lookup(hashedQueries)

    // Finally, transform the lookupResults RDD into the desired output format by
    // removing the signature and returning an RDD of (keyword list, result) pairs.
    val finalResults = lookupResults.map {
      case (_, keywordList, result) => (keywordList, result)
    }

    // Return the finalResults RDD containing the (keyword list, result) pairs.
    finalResults
  }
}
