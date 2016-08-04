import org.specs2.mutable._
import org.specs2.concurrent.{ ExecutionEnv => EE }
import reactivemongo.bson._
import DefaultBSONHandlers._
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import play.api.libs.iteratee.Iteratee
import reactivemongo.api.{
  Cursor, CursorFlattener, CursorProducer, DB, QueryOpts, WrappedCursor
}

class IterateeSpec extends Specification {
  sequential

  import Common._

  val coll = db(s"iteratee${System identityHashCode this}")

  "ReactiveMongo" should {
    "insert 16,517 records" in {
      val futs = for(i <- 0 until 16517)
        yield coll.insert(BSONDocument("i" -> BSONInteger(i), "record" -> BSONString("record" + i)))
      val fut = Future.sequence(futs)
      Await.result(fut, DurationInt(20).seconds)
      println("inserted 16,517 records")
      success
    }

    "get all the 16,517 documents" in { implicit ee: EE =>
      var i = 0
      val future = coll.find(BSONDocument.empty).cursor.enumerate() |>>> (Iteratee.foreach({ e =>
        //println(s"doc $i => $e")
        i += 1
      }))
      /*val future = coll.find(BSONDocument()).cursor.documentStream.map { doc =>
        i += 1
        println("fetched " + doc)
        doc
       }.runLast*/
      future.map(_ => i) must beEqualTo(16517).await(0, 21.seconds)
    }
  }

  "BSON Cursor" should {
    object IdReader extends BSONDocumentReader[Int] {
      def read(doc: BSONDocument): Int = doc.getAs[Int]("id").get
    }

    val expectedList = List(9, 8, 7, 6, 5, 4, 3, 2, 1, 0)
    @inline def toList =
      Iteratee.fold[Int, List[Int]](List.empty[Int]) { (l, i) => i :: l }

    "read from collection" >> {
      def collection(n: String) = {
        val col = db(s"colliter_$n")

        Future.sequence((0 until 10) map { id =>
          col.insert(BSONDocument("id" -> id))
        }) map { _ =>
          println(s"-- all documents inserted in test collection $n")
          col
        }
      }

      @inline def cursor(n: String): Cursor[Int] = {
        implicit val reader = IdReader
        Cursor.flatten(collection(n).map(_.find(BSONDocument()).
          sort(BSONDocument("id" -> 1)).cursor[Int]))
      }

      "successfully using cursor" in { implicit ee: EE =>
        (cursor("senum1").enumerate(10) |>>> toList).
          aka("enumerated") must beEqualTo(expectedList).await(0, timeout)
      }
    }

    "read from capped collection" >> {
      def collection(n: String, database: DB) = {
        val col = database(s"somecollection_captail_$n")

        col.createCapped(4096, Some(10)) map { _ =>
          (0 until 10).foreach { id =>
            col.insert(BSONDocument("id" -> id))
            Thread.sleep(200)
          }
          println(s"-- all documents inserted in test collection $n")
        }

        col
      }

      @inline def tailable(n: String, database: DB = db) = {
        implicit val reader = IdReader
        collection(n, database).find(BSONDocument()).options(
          QueryOpts().tailable).cursor[Int]
      }

      "successfully using tailable enumerator with maxDocs" in {
        implicit ee: EE =>
        (tailable("tenum1").enumerate(10) |>>> toList).
          aka("enumerated") must beEqualTo(expectedList).await(0, timeout)
      }

      "with timeout using tailable enumerator w/o maxDocs" in {
        Await.result(tailable("tenum2").enumerate() |>>> toList, timeout).
          aka("enumerated") must throwA[Exception]
      }
    }
  }

  trait FooCursor[T] extends Cursor[T] { def foo: String }

  class DefaultFooCursor[T](val wrappee: Cursor[T])
      extends FooCursor[T] with WrappedCursor[T] {
    val foo = "Bar"
  }

  class FlattenedFooCursor[T](cursor: Future[FooCursor[T]])
      extends reactivemongo.api.FlattenedCursor[T](cursor) with FooCursor[T] {

    val foo = "raB"
  }
}
