object Common {
  import scala.concurrent._
  import scala.concurrent.duration._
  import reactivemongo.api._

  implicit val ec = ExecutionContext.Implicits.global

  val logger = reactivemongo.util.LazyLogger("tests")

  val replSetOn =
    Option(System getProperty "test.replicaSet").fold(false) {
      case "true" => true
      case _      => false
    }

  val primaryHost =
    Option(System getProperty "test.primaryHost").getOrElse("localhost:27017")

  val failoverRetries = Option(System getProperty "test.failoverRetries").
    flatMap(r => scala.util.Try(r.toInt).toOption).getOrElse(7)

  @volatile private var driverStarted = false
  lazy val driver = {
    val d = new MongoDriver
    driverStarted = true
    d
  }

  val failoverStrategy = FailoverStrategy(retries = failoverRetries)

  val DefaultOptions = {
    val opts = MongoConnectionOptions(
      failoverStrategy = failoverStrategy,
      nbChannelsPerNode = 20
    )

    if (Option(System getProperty "test.enableSSL").exists(_ == "true")) {
      opts.copy(sslEnabled = true, sslAllowsInvalidCert = true)
    } else opts
  }

  private val timeoutFactor = 1.25D
  def estTimeout(fos: FailoverStrategy): FiniteDuration =
    (1 to fos.retries).foldLeft(fos.initialDelay) { (d, i) =>
      d + (fos.initialDelay * ((timeoutFactor * fos.delayFactor(i)).toLong))
    }

  val timeout: FiniteDuration = {
    val maxTimeout = estTimeout(failoverStrategy)

    if (maxTimeout < 10.seconds) 10.seconds
    else maxTimeout
  }
  val timeoutMillis = timeout.toMillis.toInt

  lazy val connection = driver.connection(List(primaryHost), DefaultOptions)
  lazy val db = Await.result(
    connection.database("specs2-test-reactivemongo").flatMap({ _db =>
      _db.drop().map(_ => _db)
    }), timeout
  )

  def close(): Unit = {
    if (driverStarted) {
      try {
        driver.close()
      } catch {
        case e: Throwable =>
          logger.warn(s"Fails to stop the default driver: $e")
          logger.debug("Fails to stop the default driver", e)
      }
    }
  }
}
