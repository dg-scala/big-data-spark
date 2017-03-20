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
    val posts = sc.parallelize(Seq.empty[Posting])
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

  test("scoredPostings work for empty groupPostings") {
    import StackOverflow._
    val posts = sc.parallelize(Seq.empty[Posting])
    val grouped = testObject.groupedPostings(posts)
    assert(testObject.scoredPostings(grouped).isEmpty(), "scoredPostings shoudl work for empty groupPostings")
  }

  test("scoredPostings get the right scores") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(
      Posting(2, 1, None, Some(2), 0, Some("Java")),
      Posting(1, 2, None, None, 3, Some("Java")),
      Posting(2, 3, None, Some(2), 5, Some("Java")),
      Posting(1, 4, None, None, 2, Some("Scala")),
      Posting(2, 5, None, Some(4), 2, Some("Scala")),
      Posting(1, 6, None, None, 2, Some("JavaScript")),
      Posting(2, 7, None, None, 4, Some("C"))
    ))
    val grouped = testObject.groupedPostings(posts)
    val scored = testObject.scoredPostings(grouped)

    val actual = scored.collect().sortWith((a, b) => a._2 > b._2)
    assert(actual.sameElements(Array(
      (Posting(1, 2, None, None, 3, Some("Java")), 5),
      (Posting(1, 4, None, None, 2, Some("Scala")), 2)
    )), "scoredPostings should get the highest scores for groupedPostings")
  }

  test("empty vectorPostings from empty scoredPostings") {
    import StackOverflow._
    val posts = sc.parallelize(Seq.empty[Posting])

    assert(
      testObject.vectorPostings(scoredPostings(groupedPostings(posts))).isEmpty(),
      "vectorPostings should be empty for empty Postings RDD")
  }

  test("empty vectorPostings from non empty scoredPostings") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(
      Posting(1, 1, None, None, 3, None),
      Posting(2, 2, None, Some(1), 5, Some("Java"))
    ))
    val grouped = testObject.groupedPostings(posts)
    val scored = testObject.scoredPostings(grouped)
    assert(scored.count() == 1, "scored should have one element")

    val vector = testObject.vectorPostings(scored)
    assert(vector.isEmpty(), "vectorPostings should be empty for scored postings with no language in tag")
  }

  test("vectorPostings produce correct vectors") {
    import StackOverflow._
    val posts = sc.parallelize(Seq(
      Posting(2, 1, None, Some(2), 0, Some("Java")),
      Posting(1, 2, None, None, 3, Some("Java")),
      Posting(2, 3, None, Some(2), 5, Some("Java")),
      Posting(1, 4, None, None, 2, Some("Scala")),
      Posting(2, 5, None, Some(4), 2, Some("Scala")),
      Posting(1, 6, None, None, 2, Some("JavaScript")),
      Posting(2, 7, None, None, 4, Some("C"))
    ))
    val grouped = testObject.groupedPostings(posts)
    val scored = testObject.scoredPostings(grouped)
    val vectors = testObject.vectorPostings(scored)

    val actual = vectors.collect().sortWith((x, y) => x._1 > x._2)
    assert(
      actual.sameElements(Array((50000, 5), (500000, 2))),
      "vectorPostings should produce correct vectors for scoredPostings")
  }

}
