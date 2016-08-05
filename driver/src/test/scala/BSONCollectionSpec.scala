import scala.util.{ Failure, Success }
import reactivemongo.api._
import reactivemongo.bson._
import scala.concurrent._
import org.specs2.mutable.Specification
import play.api.libs.iteratee.Iteratee

import org.specs2.concurrent.{ ExecutionEnv => EE }

class BSONCollectionSpec extends Specification {
  import Common._

  sequential

  import reactivemongo.api.collections.bson._

  lazy val collection = db("somecollection_bsoncollectionspec")

  case class Person(name: String, age: Int)
  case class CustomException(msg: String) extends Exception(msg)

  object BuggyPersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      throw CustomException("PersonWrite error")
  }

  object BuggyPersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = throw CustomException("hey hey hey")
  }

  class SometimesBuggyPersonReader extends BSONDocumentReader[Person] {
    var i = 0
    def read(doc: BSONDocument): Person = {
      i += 1
      if (i % 4 == 0)
        throw CustomException("hey hey hey")
      else Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
    }
  }

  object PersonWriter extends BSONDocumentWriter[Person] {
    def write(p: Person): BSONDocument =
      BSONDocument("age" -> p.age, "name" -> p.name)
  }

  object PersonReader extends BSONDocumentReader[Person] {
    def read(doc: BSONDocument): Person = Person(doc.getAs[String]("name").get, doc.getAs[Int]("age").get)
  }

  val person = Person("Jack", 25)
  val person2 = Person("James", 16)
  val person3 = Person("John", 34)
  val person4 = Person("Jane", 24)
  val person5 = Person("Joline", 34)

  "BSONCollection" should {
    "write five docs with success" in {
      implicit val writer = PersonWriter
      Await.result(collection.insert(person), timeout).ok mustEqual true
      Await.result(collection.insert(person2), timeout).ok mustEqual true
      Await.result(collection.insert(person3), timeout).ok mustEqual true
      Await.result(collection.insert(person4), timeout).ok mustEqual true
      Await.result(collection.insert(person5), timeout).ok mustEqual true
    }

    "read empty cursor" >> {
      @inline def cursor: Cursor[BSONDocument] =
        collection.find(BSONDocument("plop" -> "plop")).cursor[BSONDocument]

      "with success using collect" in {
        val list = cursor.collect[Vector](10)
        Await.result(list, timeout).length mustEqual 0
      }

      "with success using enumerate" in {
        val enumerator = cursor.enumerate(10)
        val n = enumerator |>>> Iteratee.fold(0) { (r, doc) =>
          r + 1
        }
        Await.result(n, timeout) mustEqual 0
      }

      "with success using foldResponses" in { implicit ee: EE =>
        cursor.foldResponses(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)
        ).
          aka("result") must beEqualTo(1 /* one empty response */ ).
          await(0, timeout)

      }

      "with success using foldBulks" in { implicit ee: EE =>
        cursor.foldBulks(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)
        ).
          aka("result") must beEqualTo(1 /* one empty response */ ).
          await(0, timeout)

      }

      "with success using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)(
          (i, _) => Cursor.Cont(i + 1), (_, e) => Cursor.Fail(e)
        ).
          aka("result") must beEqualTo(0).await(0, timeout)

      }

      "with success as option" in { implicit ee: EE =>
        cursor.headOption must beNone.await(0, timeout)
      }
    }

    "read a doc with success" in {
      implicit val reader = PersonReader
      Await.result(collection.find(BSONDocument()).one[Person], timeout).get mustEqual person
    }

    "read all with success" >> {
      implicit val reader = PersonReader
      @inline def cursor = collection.find(BSONDocument()).cursor[Person]
      val persons = Seq(person, person2, person3, person4, person5)

      "as list" in { implicit ee: EE =>
        (cursor.collect[List]() must beEqualTo(persons).await(0, timeout)).
          and(cursor.headOption must beSome(person).await(0, timeout))
      }

      "using foldResponses" in { implicit ee: EE =>
        cursor.foldResponses(0)(
          { (s, _) => Cursor.Cont(s + 1) },
          (_, e) => Cursor.Fail(e)
        ) must beEqualTo(1).await(0, timeout)

      }

      "using foldBulks" in { implicit ee: EE =>
        cursor.foldBulks(1)(
          { (s, _) => Cursor.Cont(s + 1) },
          (_, e) => Cursor.Fail(e)
        ) must beEqualTo(2).await(0, timeout)

      }

      "using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(Nil: Seq[Person])(
          (s, p) => Cursor.Cont(s :+ p),
          (_, e) => Cursor.Fail(e)
        ) must beEqualTo(persons).await(0, timeout)

      }
    }

    "read until John" in { implicit ee: EE =>
      implicit val reader = PersonReader
      @inline def cursor = collection.find(BSONDocument()).cursor[Person]
      val persons = Seq(person, person2, person3)

      cursor.foldWhile(Nil: Seq[Person])({ (s, p) =>
        if (p.name == "John") Cursor.Done(s :+ p)
        else Cursor.Cont(s :+ p)
      }, (_, e) => Cursor.Fail(e)) must beEqualTo(persons).await(0, timeout)
    }

    "read a doc with error" in {
      implicit val reader = BuggyPersonReader
      val future = collection.find(BSONDocument()).one[Person].map(_ => 0).recover {
        case e if e.getMessage == "hey hey hey" => -1
        case e =>
          e.printStackTrace()
          -2
      }
      val r = Await.result(future, timeout)
      println(s"read a doc with error: $r")
      Await.result(future, timeout) mustEqual -1
    }

    "read docs with error" >> {
      implicit val reader = new SometimesBuggyPersonReader
      @inline def cursor = collection.find(BSONDocument()).cursor[Person]

      "using collect" in { implicit ee: EE =>
        val collect = cursor.collect[Vector]().map(_.size).recover {
          case e if e.getMessage == "hey hey hey" => -1
          case e                                  => e.printStackTrace(); -2
        }

        collect aka "first collect" must not(throwA[Exception]).
          await(0, timeout) and (collect must beEqualTo(-1).
            await(0, timeout))
      }

      "using foldWhile" in {
        Await.result(cursor.foldWhile(0)(
          (i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Fail(e)
        ), timeout) must throwA[CustomException]
      }

      "fallbacking to final value using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)(
          (i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Done(-1)
        ) must beEqualTo(-1).await(0, timeout)
      }

      "skiping failure using foldWhile" in { implicit ee: EE =>
        cursor.foldWhile(0)(
          (i, _) => Cursor.Cont(i + 1),
          (_, e) => Cursor.Cont(-3)
        ) must beEqualTo(-2).await(0, timeout)
      }
    }

    "read docs until error" in {
      implicit val reader = new SometimesBuggyPersonReader
      val enumerator = collection.find(BSONDocument()).cursor[Person].enumerate(stopOnError = true)
      var i = 0
      val future = enumerator |>>> Iteratee.foreach { doc =>
        i += 1
        println(s"\tgot doc: $doc")
      } map (_ => -1)
      val r = Await.result(future.recover { case e => i }, timeout)
      println(s"read $r/5 docs (expected 3/5)")
      r mustEqual 3
    }

    "read docs skipping errors" in {
      implicit val reader = new SometimesBuggyPersonReader
      val enumerator = collection.find(BSONDocument()).cursor[Person].enumerate(stopOnError = false)
      var i = 0
      val future = enumerator |>>> Iteratee.foreach { doc =>
        i += 1
        println(s"\t(skipping [$i]) got doc: $doc")
      }
      val r = Await.result(future, timeout)
      println(s"read $i/5 docs (expected 4/5)")
      i mustEqual 4
    }
    "read docs skipping errors using collect" in {
      implicit val reader = new SometimesBuggyPersonReader
      val result = Await.result(collection.find(BSONDocument()).cursor[Person].collect[Vector](stopOnError = false), timeout)
      println(s"(read docs skipping errors using collect) got result $result")
      result.length mustEqual 4
    }

    "write a doc with error" in { implicit ee: EE =>
      implicit val writer = BuggyPersonWriter

      collection.insert(person).map { lastError =>
        println(s"person write succeed??  $lastError")
        0
      }.recover {
        case ce: CustomException => -1
        case e =>
          e.printStackTrace()
          -2
      } aka "write result" must beEqualTo(-1).await(0, timeout)
    }
  }

  "Index" should {
    import reactivemongo.api.indexes._
    val col = db(s"indexed_col_${hashCode}")

    "be first created" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true
      )).
        aka("index creation") must beTrue.await(0, timeout)
    }

    "not be created if already exists" in { implicit ee: EE =>
      col.indexesManager.ensure(Index(
        Seq("token" -> IndexType.Ascending), unique = true
      )).
        aka("index creation") must beFalse.await(0, timeout)

    }
  }
}
