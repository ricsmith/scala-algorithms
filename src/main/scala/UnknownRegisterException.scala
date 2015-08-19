package rs.analytics.datastructures.probabilistic.hll

/**
 * Created by ralsmith on 8/17/15.
 */
class UnknownRegisterException(message: String = null) extends Exception(message) {}

object UnknownRegisterException {
  def create(msg: String) : UnknownRegisterException = new UnknownRegisterException(msg)

  def create(msg: String, cause: Throwable) = new UnknownRegisterException(msg).initCause(cause)
}
