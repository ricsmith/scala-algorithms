
package rs.analytics.datastructures.cardinality

/**
 * Created by ralsmith on 8/6/15.
 */
trait Cardinality {

  def offer(value: Int): Boolean

  def offer(value: Long): Boolean

  def offer(value: String): Boolean

  def cardinality(): Long

  def merge[T <:Cardinality](estimators : T*) : Cardinality

}