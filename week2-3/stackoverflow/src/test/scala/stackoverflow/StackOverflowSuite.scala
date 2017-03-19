package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.net.URL
import java.nio.channels.Channels
import java.io.File
import java.io.FileOutputStream

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings returns empty RDD") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(Posting(2, 1, None, None, 0, Some("Java"))))
    assert(testObject.groupedPostings(posts).isEmpty(), "groupedPosting should be empty if just answers are supplied.")
  }

  test("groupedPostings ignore dangling questions and answers") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(
      Posting(2, 1, None, None, 0, Some("Java")),
      Posting(1, 2, None, None, 3, Some("Scala"))))
    assert(testObject.groupedPostings(posts).isEmpty(), "groupedPosting should ignore dangling questions and answers.")
  }

  test("groupedPostings group correctly") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(
      Posting(2, 1, None, Some(2), 0, Some("Java")),
      Posting(1, 2, None, None, 3, Some("Scala")),
      Posting(1, 3, None, None, 2, Some("Scala")),
      Posting(2, 4, None, None, 4, Some("C"))))
    val grouped = testObject.groupedPostings(posts)
    assert(grouped.count() == 1, "groupedPostings group correctly - count")
    assert(grouped.collectAsMap().keys == Set(2), "groupedPostings group correctly - keys")
  }

}
