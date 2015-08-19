package rs.analytics.datastructures.probabilistic

import rs.analytics.datastructures.cardinality.Cardinality
import rs.analytics.datastructures.probabilistic.hll.{HyperLogLog, Register, HyperLogLogMergeException}

import scala.collection.mutable.HashSet
import scala.util.Random

/**
 * Created by ralsmith on 8/5/15.
 */
class HyperLogLogSpec extends UnitSpec {

  "Adding a single postive value to an empty set" should "result in a cardinality of one" in {
    for(enum <- Register.REGISTER_SET_TYPE.values) {
      val hll: HyperLogLog = new HyperLogLog(Option(14), Option(5), Option(enum))
      hll.offer(1L)
      val card = hll.cardinality
      card should be(1L)
    }
  }

  "Adding a single negative value to an empty set" should "result in a cardinality of one" in {
    val hll: HyperLogLog = new HyperLogLog()
    hll.offer(-1L)
    hll.cardinality should be(1L)
  }

  "Adding a duplicate value to an empty set" should "result in a cardinality of one" in {
    val hll: HyperLogLog = new HyperLogLog()
    hll.offer(1L)
    hll.cardinality should be(1L)
    hll.offer(1L)
    hll.cardinality should be(1L)
  }

  "What you put into a register" should  "you be able to get out of a register" in {
    val register: Register = Register.apply(Register.REGISTER_SET_TYPE.DENSE_ARRAY)
    val count: Int = (register.m.toInt)
    val random: Random = new Random(1)
    for(j <- 0 until 100; i <- 0 until count){
      val newVal = Math.abs(random.nextInt(31))
      register.set(i, newVal)
      val value = register.get(i)
      assert(value == newVal)
    }
  }

  "What you update a register" should  "you be able to get out of a register" in {
    val register: Register = Register.apply(Register.REGISTER_SET_TYPE.DENSE_ARRAY)
    val count: Int = (register.m.toInt)
    val random: Random = new Random(1)
    for(j <- 0 until 2){
      for(i <- 0 until count) {
        val newVal = Math.abs(random.nextInt(31))
        val prevalue = register.get(i)
        register.upsert(i, newVal)
        val updatedValue = register.get(i)
        assert(updatedValue == newVal || updatedValue == prevalue)
      }
    }
  }

  "A register upsert " should  "not corrupt the register" in {
    // fix bug where registerValueMask was trunctated as an Int, value is now a long
    val register: Register = Register.apply(Register.REGISTER_SET_TYPE.DENSE_ARRAY)
    register.upsert(21, 3)
    val firstUpdate = register.get(21)
    register.upsert(21, 8)
    val secoundUpdate = register.get(21)
    assert(firstUpdate == 3 && secoundUpdate == 8)
  }

  "The cardinality of HLL and the canonical cardinality of a set" should "generate a pct error that is less than or equal to the std error of the HLL object" in {
    for(enum <- Register.REGISTER_SET_TYPE.values) {
      val canonical: HashSet[Long] = new HashSet[Long]()
      val hll: HyperLogLog = new HyperLogLog(Option(14), Option(5), Option(enum))
      val seed: Long = 1L // constant so results are reproducible
      val random: Random = new Random(seed)
      val itr = 100000
      for (i <- 0 until itr) {
        val randomLong: Long = random.nextLong()
        canonical.add(randomLong)
        hll.offer(randomLong)
      }

      val canonicalCardinality: Int = canonical.size
      val t0 = System.nanoTime()
      val estimatedCardinality: Long = hll.cardinality()
      val t1 = System.nanoTime()

      val highRange: Double = canonicalCardinality + (hll.stdError()*(canonicalCardinality))
      val lowRange: Double = canonicalCardinality - (hll.stdError()*(canonicalCardinality))

      assert(highRange >= estimatedCardinality && lowRange <= estimatedCardinality )
    }
  }

  "When merging two HHL objects we " should "observe roughly the combined cardinality of the two objects." in {
    for(enum <- Register.REGISTER_SET_TYPE.values) {
      val hll1: HyperLogLog = new HyperLogLog()
      val trueCard = 100
      val canonical: HashSet[Long] = new HashSet[Long]()
      for (value <- 1 to trueCard) {
        hll1.offer(value)
        canonical.add(value)
      }

      val card1: Long = hll1.cardinality()

      val hll2: HyperLogLog = new HyperLogLog()
      for (value <- (trueCard + 1) to (2 * trueCard)) {
        hll2.offer(value)
        canonical.add(value)
      }

      val card2: Long = hll2.cardinality()

      val c: Cardinality = hll1.merge(hll2)

      val card3: Long = c.cardinality()

      val highRange: Double = 2*trueCard + (hll1.stdError()*(2*trueCard))
      val lowRange: Double = 2*trueCard - (hll1.stdError()*(2*trueCard))

      assert(highRange >= card3 && lowRange <= card3 )
    }
  }

  "A small range smoke test" should "complete successfully." in {
    val log2m: Int = 11
    val hll: HyperLogLog = new HyperLogLog(Option(log2m))
    val canonicalCardinality: Int = (1 << log2m) - 100
    for (i <- 0 until canonicalCardinality) {
      hll.offer(i)
    }

    val estimatedCardinality = hll.cardinality()

    val highRange: Double = canonicalCardinality + (hll.stdError()*(canonicalCardinality))
    val lowRange: Double = canonicalCardinality - (hll.stdError()*(canonicalCardinality))

    assert(highRange >= estimatedCardinality && lowRange <= estimatedCardinality )
  }

  "A normal range smoke test" should "complete successfully." in {
    val log2m: Int = 11
    val hll: HyperLogLog = new HyperLogLog(Option(log2m))
    val canonicalCardinality: Int = 3 * (1 << log2m)
    for (i <- 0 until canonicalCardinality) {
      hll.offer(i)
    }

    val estimatedCardinality = hll.cardinality()

    val highRange: Double = canonicalCardinality + (hll.stdError()*(canonicalCardinality))
    val lowRange: Double = canonicalCardinality - (hll.stdError()*(canonicalCardinality))

    assert(highRange >= estimatedCardinality && lowRange <= estimatedCardinality )
  }

}
