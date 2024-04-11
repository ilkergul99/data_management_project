package app.recommender.LSH


import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/**
 * Class for indexing the data for LSH
 *
 * @param data The title data to index
 * @param seed The seed to use for hashing keyword lists
 */
class LSHIndex(data: RDD[(Int, String, List[String])], seed : IndexedSeq[Int]) extends Serializable {

  private val minhash = new MinHash(seed)

  // Compute the hash for each title and group titles with the same hash (signature)
  private val buckets: RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = data.map {
    case (titleId, titleName, titleKeywords) =>
      // Compute the hash or signature for each title based on its keywords
      val signature = minhash.hash(titleKeywords)
      // Associate the title with its signature
      (signature, (titleId, titleName, titleKeywords))
  }.groupByKey().mapValues(_.toList)

  // Cache the buckets RDD for subsequent uses
  buckets.persist(MEMORY_AND_DISK)

  /**
   * Hash function for an RDD of queries.
   *
   * @param input The RDD of keyword lists
   * @return The RDD of (signature, keyword list) pairs
   */
  def hash(input: RDD[List[String]]): RDD[(IndexedSeq[Int], List[String])] = {
    // Use the 'map' function to every element in the RDD
    input.map(x =>
      // For each element (which is a List[String]), create a tuple
      // The first element of the tuple is the hash of the list
      // Use minhash.hash function to compute the hash
      // The second element of the tuple is the original list
      (minhash.hash(x), x)
    )
  }

  /**
   * Return data structure of LSH index for testing
   *
   * @return Data structure of LSH index
   */
  def getBuckets(): RDD[(IndexedSeq[Int], List[(Int, String, List[String])])] = {
    // This function simply returns the variable 'buckets' that has been computed
    buckets
  }

  /**
   * Lookup operation on the LSH index
   *
   * @param queries The RDD of queries. Each query contains the pre-computed signature
   *                and a payload
   * @return The RDD of (signature, payload, result) triplets after performing the lookup.
   *         If no match exists in the LSH index, return an empty result list.
   */
  def lookup[T: ClassTag](queries: RDD[(IndexedSeq[Int], T)])
  : RDD[(IndexedSeq[Int], T, List[(Int, String, List[String])])] = {
    // Perform a join operation between the queries and the buckets RDD.
    // The join operation uses the signatures as the key for the join.
    // This will return an RDD with matched signature pairs and their corresponding payload and title data.
    val joinedResults = queries.join(buckets)

    // Transform the joinedResults RDD to the desired output format.
    // The map operation extracts the signature, payload, and title data from the joinedResults RDD
    // and returns a new RDD with the desired format (signature, payload, title data).
    val finalResults = joinedResults.map {
      case (signature, (payload, titleData)) => (signature, payload, titleData)
    }

    // Return the finalResults RDD, which contains the (signature, payload, title data) triplets.
    finalResults
  }
}
