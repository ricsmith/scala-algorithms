package rs.analytics.datastructures.probabilistic.hll

import rs.analytics.datastructures.cardinality.CardinalityMergeException

/**
 * Created by ralsmith on 8/6/15.
 */
class HyperLogLogMergeException(message: String = null) extends CardinalityMergeException(message) {}

object HyperLogLogMergeException {
  def create(msg: String) : HyperLogLogMergeException = new HyperLogLogMergeException(msg)

  def create(msg: String, cause: Throwable) = new HyperLogLogMergeException(msg).initCause(cause)
}
