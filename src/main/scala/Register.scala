package rs.analytics.datastructures.probabilistic.hll

/**
 * Created by ralsmith on 8/5/15.
 */
sealed trait Register {

  lazy val _registerWidth: Int = Register.DEFAULT_REGISTER_WIDTH
  lazy val _log2m: Int = Register.DEFAULT_LOG2M
  lazy val _mBitMask: Long = m - 1
  lazy val _maxRegValBitMask: Long = Register.maxRegisterValueBitMask(_registerWidth)
  lazy val _pwMaxBitMask: Long =  ~((1L << (((1 << _registerWidth) - 1) - 1)) - 1)

  var numZeros: Long = m

  def add(hashedValue: Long): Boolean ={
    var result: Boolean = false
    val regIdx: Long = hashedValue & _mBitMask
    val w: Long = hashedValue >>> log2m

    var value: Long  = 0
    if(w != 0L) {
      value = java.lang.Long.numberOfTrailingZeros(w | _pwMaxBitMask) + 1
    }

    if(value != 0) {
      result = upsert(regIdx, value)
    }

    result
  }

  def get(position: Long): Long

  def upsert(position: Long, value: Long): Boolean

  def set(position: Long, value: Long)

  def merge(that: Register)

  def m: Long= {
    1 << log2m
  }

  def log2m: Int ={
    _log2m
  }

  def width: Int={
    _registerWidth
  }

  def sumInversePowerOf2(): Double={
    var registerSum: Double = 0
    val c: Long = m
    var j=0
    while(j < c) {
      val value: Long = get(j)
      registerSum += 1.0 / (1L << value)
      j += 1
    }
    registerSum
  }
}

object Register {

  val MINIMUM_LOG2M: Int = 4
  val MAXIMUM_LOG2M: Int = 30

  val MINIMUM_REGWIDTH: Int = 1
  val MAXIMUM_REGWIDTH: Int = 8

  val LOG2_BITS_PER_WORD: Int = 6
  val BITS_PER_WORD: Int = 1 << LOG2_BITS_PER_WORD
  val BITS_PER_WORD_MASK: Int = BITS_PER_WORD - 1

  val DEFAULT_REGISTER_WIDTH: Int = 5
  val DEFAULT_LOG2M: Int = 14

  object REGISTER_SET_TYPE extends Enumeration {
    type REGISTER_SET_TYPE = Value
    val DENSE_ARRAY = Value //, DENSE_ARRAY_OFFHEAP = Value//DENSE, SPARSE = Value
  }

  @throws(classOf[UnknownRegisterException])
  @throws(classOf[IllegalArgumentException])
  def apply(regSetType: REGISTER_SET_TYPE.REGISTER_SET_TYPE,
            log2mOp: Option[Int] = Some(Register.DEFAULT_LOG2M),
            registerWidth: Option[Int] = Some(Register.DEFAULT_REGISTER_WIDTH)): Register = {

    if(log2mOp.get  < Register.MINIMUM_LOG2M && log2mOp.get > Register.MAXIMUM_LOG2M) {
      throw new IllegalArgumentException(s"_log2m - Must be greater than or equal to ${Register.MINIMUM_LOG2M} and less than ${Register.MAXIMUM_LOG2M}.")
    }

    if(registerWidth.get  < Register.MINIMUM_REGWIDTH && registerWidth.get > Register.MAXIMUM_REGWIDTH) {
      throw new IllegalArgumentException(s"_log2m - Must be greater than or equal to ${Register.MINIMUM_REGWIDTH} and less than ${Register.MAXIMUM_REGWIDTH}.")
    }

    val result: Register = regSetType match {
      case REGISTER_SET_TYPE.DENSE_ARRAY => new DenseArrayRegister(log2mOp, registerWidth)
      // case REGISTER_SET_TYPE.DENSE_ARRAY_OFFHEAP => new OffHeapRegister(log2m)
      case _ => throw new UnknownRegisterException(s"${regSetType} is unknown.")
    }
    result
  }

  def maxRegisterValueBitMask(registerWidth: Int): Long ={
    (1L << registerWidth) - 1
  }

  private class DenseArrayRegister(val _log2mOp: Option[Int] = Some(Register.DEFAULT_LOG2M),
                                   val _registsterWidthOp: Option[Int] = Some(Register.DEFAULT_REGISTER_WIDTH)) extends Register {

    override lazy val _log2m = _log2mOp.get
    override lazy val _registerWidth: Int = _registsterWidthOp.get

    val _mArray: Array[Long] = new Array[Long](ArrayRegister.calculateArraySize(m))

    @inline
    def get(regIdx: Long): Long = {
      val tuple: (Long, Int, Int, Int) = getRegTuple(regIdx)
      var result: Long = 0
      if(tuple._2 == tuple._3)
        result = (_mArray(tuple._2) >>> tuple._4) & _maxRegValBitMask
      else
        result = (_mArray(tuple._2) >>> tuple._4) | (_mArray(tuple._3) << (Register.BITS_PER_WORD - tuple._4)) & _maxRegValBitMask
      result
    }

    def upsert(regIdx: Long, value: Long): Boolean = {
      var result: Boolean = false
      val regValue: Long = get(regIdx)

      if(value > regValue){
        if(regValue == 0 && numZeros != 0) {
          numZeros -= 1
        }
        set(regIdx, value)
        result = true
      }
      result
    }

    @inline
    def set(regIdx: Long, value: Long) = {
      val tuple: (Long, Int, Int, Int) = getRegTuple(regIdx)
      if(tuple._2 == tuple._3) {
        _mArray(tuple._2) &= ~(_maxRegValBitMask << tuple._4) // clear the register
        _mArray(tuple._2) |= (value << tuple._4) // set the register
      } else {
        _mArray(tuple._2) &= (1L << tuple._4) - 1
        _mArray(tuple._2) |= (value << tuple._4)
        _mArray(tuple._3) &= ~(_maxRegValBitMask >>> (Register.BITS_PER_WORD - tuple._4))
        _mArray(tuple._3) |= (value >>> (Register.BITS_PER_WORD - tuple._4))
      }
    }

    def iterator(): Iterator[Long] = new RegisterIterator()

    override def sumInversePowerOf2(): Double={
      var registerSum: Double = 0
      // var zeroCount: Long = 0
      var regCount: Long = 0

      var i = 0
      while(i < m) {
        val value: Long = get(i)
        registerSum += 1.0 / (1L << value)
        regCount += 1
        // if(value == 0L) zeroCount += 1
        i += 1
      }

      registerSum
    }

    @throws(classOf[HyperLogLogMergeException])
    override def merge(that: Register) = {
      if(m != that.m){
        throw new HyperLogLogMergeException(s"m of 'this' equals ${this.m}, which does not match m of 'that' ${that.m}")
      }

      var i = 0
      val c = m
      while(i < c) {
        upsert(i, that.get(i))
        i += 1
      }
    }

    @inline
    private def getRegTuple(regIdx: Long): (Long, Int, Int, Int) ={
      val bitIdx: Long = regIdx * _registerWidth
      val wordIdx1: Int = (bitIdx >>> Register.LOG2_BITS_PER_WORD).toInt
      val wordIdx2: Int = ((bitIdx + _registerWidth - 1) >>> Register.LOG2_BITS_PER_WORD).toInt
      val remainder: Int = (bitIdx & Register.BITS_PER_WORD_MASK).toInt
      (bitIdx, wordIdx1, wordIdx2, remainder)
    }

    class RegisterIterator() extends Iterator[Long] {
      // register setup
      var regIdx: Int = 0
      val count: Long = DenseArrayRegister.this.m

      def next() = {
        val result: Long = get(regIdx)
        regIdx += 1
        result
      }

      def hasNext() = {
        regIdx < count
      }
    }

  }

  object ArrayRegister {
    @inline
    def calculateArraySize(count: Long, width: Option[Int] = Some(Register.DEFAULT_REGISTER_WIDTH)): Int ={
      (((width.get * count) + Register.BITS_PER_WORD_MASK) >>> Register.LOG2_BITS_PER_WORD).toInt
    }
  }

}