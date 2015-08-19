package rs.analytics.datastructures.probabilistic.hll

import rs.analytics.datastructures.cardinality.Cardinality
import scala.util.hashing.MurmurHash3
import java.nio.ByteBuffer

/**
 * Created by ralsmith on 8/2/15.
 */
@throws(classOf[IllegalArgumentException])
class HyperLogLog(_log2mOp: Option[Int] = Some(Register.DEFAULT_LOG2M), _registerWidthOp: Option[Int] = Some(Register.DEFAULT_REGISTER_WIDTH), _registerSetType: Option[Register.REGISTER_SET_TYPE.Value] = Some(Register.REGISTER_SET_TYPE.DENSE_ARRAY)) extends Cardinality {

  val _register: Register = Register.apply(_registerSetType.get, _log2mOp, _registerWidthOp)

  val ALPHA_MM: Double = HyperLogLog.getAlphaMM(_register.log2m, _register.m)
  val LARGE_RANGE_ESTIMATE_LIMIT: Double = HyperLogLog.largeRangeCorrectionLimit(_register.log2m, _register.width)
  val SMALL_RANGE_ESTIMATE_LIMIT: Double = HyperLogLog.smallRangeCorrectionLimit(_register.m)

  def offer(value: Int): Boolean={
    val x: Long = MurmurHash3.bytesHash(HyperLogLog.int2Bytes(value))
    _register.add(x)
  }

  def offer(value: Long): Boolean={
    val x: Long = MurmurHash3.bytesHash(HyperLogLog.long2Bytes(value))
    _register.add(x)
  }

  def offer(value: String): Boolean={
    val x: Long = MurmurHash3.stringHash(value)
    _register.add(x)
  }

  def cardinality(): Long = {
    val registerSum: Double = _register.sumInversePowerOf2()
    val count: Long = _register.m
    var estimate: Double = ALPHA_MM / registerSum

    if (_register.numZeros != 0 && estimate < SMALL_RANGE_ESTIMATE_LIMIT) {
      estimate = HyperLogLog.linearCounting(count, _register.numZeros)
    } else if ( estimate > LARGE_RANGE_ESTIMATE_LIMIT){
      estimate = HyperLogLog.largeRangeCorrectionEst(_register.log2m, _register.width, estimate)
    }
    
    Math.round(estimate)
  }

  @inline
  private def merge(other: HyperLogLog): Either[HyperLogLogMergeException, HyperLogLog] = {
    if (_register.m != other._register.m) {
      Left(new HyperLogLogMergeException("HyperLogLog objects must have equivalent size."))
    } else {
      _register.merge(other._register)
      Right(this)
    }
  }

  @throws(classOf[HyperLogLogMergeException])
  override def merge[T <: Cardinality](estimators: T*): Cardinality ={
    val merged: HyperLogLog = new HyperLogLog( Option(_register.log2m) )
    merged.merge(this)
    val hllEst = estimators.collect{ case x: HyperLogLog => x }
    if(estimators.length != hllEst.length){
      throw new HyperLogLogMergeException("Estimators are not all of type HyperLogLog.")
    } else {
      hllEst.foreach(merged.merge(_))
    }
    merged
  }

  def stdError(): Double ={
    HyperLogLog.stdError(_register.m)
  }

}

object HyperLogLog {


  def stdError(m: Long): Double ={
    1.06 / Math.sqrt(m)
  }

  def rsd(log2m : Int): Double ={
    1.106 / Math.sqrt(Math.exp(log2m * Math.log(2)))
  }

  def getAlphaMM(log2m: Int, m: Long): Double = log2m match {
      // Add case detection for p 1-8
      case 16 => 0.673 * m * m
      case 32 => 0.697 * m * m
      case 64 => 0.709 * m * m
      case _ => (0.7213 / (1.0 + 1.079 / m)) * m * m
  }

  def linearCounting(m: Long, V: Double): Double= {
    m * Math.log(m / V)
  }

  def smallRangeCorrectionLimit(count: Long): Double ={
    (count * 5.0) / 2
  }

  def largeRangeCorrectionLimit(log2m: Int, regWidth: Int): Double ={
    HyperLogLog.twoToL(log2m, regWidth) / 30.0
  }

  def largeRangeCorrectionEst(log2m: Int, regWidth: Int, estimator: Double): Long ={
    val twoToL: Double = HyperLogLog.twoToL(log2m, regWidth)
    (-1 * twoToL * Math.log(1.0 - (estimator/twoToL))).toLong
  }

  private def twoToL(log2m: Int, regWidth: Int): Double = {
    val maxRegisterValue: Long = Register.maxRegisterValueBitMask(regWidth)
    val pwBits: Long = maxRegisterValue - 1
    val totalBits: Long = pwBits + log2m
    Math.pow(2, totalBits)
  }

  implicit def int2Bytes(i: Int): Array[Byte] = {
    val buf = new Array[Byte](4)
    ByteBuffer
      .wrap(buf)
      .putInt(i)
    buf
  }

  implicit def long2Bytes(i: Long): Array[Byte] = {
    val buf = new Array[Byte](8)
    ByteBuffer
      .wrap(buf)
      .putLong(i)
    buf
  }
}