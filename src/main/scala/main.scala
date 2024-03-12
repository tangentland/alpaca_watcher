package org.cardinal.alpaca_watcher

import akka.stream._
import akka.stream.scaladsl._
import akka.{ Done, NotUsed }
import akka.actor.ActorSystem
import akka.util.ByteString

import scala.collection.mutable.{HashMap}
import scala.concurrent._
import scala.concurrent.duration._
import scala.util._
import java.nio.file.Paths

import alpaca.Alpaca
import alpaca.alpaca.polygonStreamingClient
import alpaca.alpaca.polygonStreamingClient.PolygonStreamAggregatePerSecond


def getAlpacaStream(symbol: String): Option[Alpaca] = {
  val alpaca = new Alpaca(accountKey="PKJJ7Y34L16IQOCOGY8Q",
                          accountSecret="KRWY3vHfYxyBuddAGPyolLUSUqEYQpVbv4GIWjEN")
  val account = alpaca.getAccount.unsafeToFuture()
  account.onComplete {
    case Failure(exception) => {
      println("Could not get account information")
      return Option()
    }
    case Success(value) =>
      if (value.account_blocked) {
        println("Account is currently restricted from trading.")
        return Option()
      }
      println(s"${value}")
      val stream: StreamingClient = alpaca.alpaca.polygonStreamingClient
      stream.subscribe(PolygonAggregatePerSecondSubscribe(symbol))
      return Option(stream)
  }
}


final case class Sma(sym: String, date: String, ap: Double)
final case class Tracking(sym: String, lastDate: String, lastAP: Double, state: String)

def toSma(psa: PolygonStreamAggregatePerSecond) = {
  Sma(sym=psa.sym, date=psa.e, ap=psa.a)
}

object SmaAckingReceiver {
  case object Ack
  case object StreamInitialized
  case object StreamCompleted
  final case class StreamFailure(ex: Throwable)
}

class SmaAckingReceiver(var target: String, var symbols: List[String]) extends Actor with ActorLogging {
  import SmaAckingReceiver._
  private var symTrack = HashMap.empty[String, Tracking]

  def receive: Receive = {
    case StreamInitialized =>
      log.info("Stream initialized!")
      // ack to allow the stream to proceed sending more elements
      sender() ! Ack
    case el: Sma =>
      log.info("Received element: {}", Sma)
      if (!symTrack.contains(el.sym)) {
        symTrack += (el.sym -> Tracking(sym=el.sym, lastDate=el.date, lastAP=el.ap, state="INIT"))
      } else {
        val t = symTrack(el.sym)
        if (t.lastDate == el.date) {
          log.info("Received duplicate element: {}", el)
        } else if (t.lastAP < el.ap) {
          symTrack += (el.sym -> Tracking(sym = el.sym, lastDate = el.date, lastAP = el.ap, state = "UP"))
          if (t.state != "UP") {
            log.info(s"Symbol ${el.sym} state changed to an UP state")
            states = symTrack.values.map(e => e.state == "UP")
            if (states.map(_.forall(_ == "true").getOrElse(false))) {
                if (symTrack.values.sizeIs == symbols.size) {
                  println(s"All tracking symbols are in an UP state - BUY $target")
                } else {
                  log.info(s"Symbol ${el.sym} not all tracking symbol are present ${symbols - symTrack.keys} - no action")
                }
            } else {
              log.info(s"Symbol ${el.sym} not all states are UP ${zip(symbols, states)} - no action")
            }
          } else {
            log.info(s"Symbol ${el.sym} UP state unchanged")
          }
        } else if (t.lastAP > el.ap) {
            symTrack += (el.sym -> Tracking(lastDate=el.date, lastAP=el.ap, state="DOWN"))
            log.info("Symbol {} state changed to an DOWN state", el.sym)
        }
      }
      // ack to allow the stream to proceed sending more elements
      sender() ! Ack
    case StreamCompleted =>
      log.info("Stream completed!")
    case StreamFailure(ex) =>
      log.error(ex, "Stream failed!")
  }
}

@main
def main(): Unit = {
  implicit val system: ActorSystem = ActorSystem("alpaca-watcher")
  val target: String = "APPL"
  val triggers = List("APPL", "IBM", "MSFT", "GOOGL", "AMZN")

  val trigger_sources = scala.collection.mutable.HashMap.empty[String, Source]
  for (tname <- triggers) {
    var stream = getAlpacaStream(symbol=tname)
    if (stream.isDefined)
      var stream = stream.get
      trigger_sources += (tname -> Source(stream._2))
  }

  val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder: GraphDSL.Builder[NotUsed] =>
    import GraphDSL.Implicits._
    // sent from actor to stream to "ack" processing of given element
    val AckMessage = AckingReceiver.Ack

    // sent from stream to actor to indicate start, end or failure of stream:
    val InitMessage = AckingReceiver.StreamInitialized
    val OnCompleteMessage = AckingReceiver.StreamCompleted
    val onErrorMessage = (ex: Throwable) => AckingReceiver.StreamFailure(ex)

    val receiver = system.actorOf(Props(new SmaAckingReceiver(target=target, symbols=triggers)))
    val sink = Sink.actorRefWithBackpressure(
      receiver,
      onInitMessage = InitMessage,
      ackMessage = AckMessage,
      onCompleteMessage = OnCompleteMessage,
      onFailureMessage = onErrorMessage)

    val merge = builder.add(Merge[Sma](trigger_sources.size))
    for (src <- trigger_sources.values){
      src ~> toSma ~> merge
    }
    merge ~> sink

    ClosedShape
  })
  g.run()
}