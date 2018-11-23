/*
 * Copyright 2018 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ibm.developer.eventstore.akka

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import akka.Done
import akka.actor.{ ActorSystem, Terminated }
import akka.event.Logging
import akka.util.ByteString
import akka.http.scaladsl.model.ws.{ BinaryMessage, Message, TextMessage }
import akka.http.scaladsl.model.{ ContentTypes, HttpEntity }
import akka.http.scaladsl.server.{ HttpApp, Route }
import akka.http.scaladsl.settings.ServerSettings
import akka.stream.{ ActorMaterializer, ActorMaterializerSettings, Supervision }
import akka.stream.alpakka.csv.scaladsl.{ CsvParsing, CsvToMap }
import akka.stream.alpakka.ibm.eventstore.scaladsl.EventStoreSink
import akka.stream.scaladsl.{ Flow, Source }
import com.ibm.event.common.ConfigurationReader
import org.apache.spark.sql.Row

import scala.concurrent._
import scala.util.Success

class OnlineRetailWebServer extends HttpApp {

  val decider: Supervision.Decider = { e =>
    println("Unhandled exception in stream", e)
    Supervision.Resume
  }

  implicit val system: ActorSystem = ActorSystem(Logging.simpleName(this).replaceAll("\\$", ""))
  val materializerSettings: ActorMaterializerSettings = ActorMaterializerSettings(system).withSupervisionStrategy(decider)
  implicit val materializer: ActorMaterializer = ActorMaterializer(materializerSettings)
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  ConfigurationReader.setConnectionEndpoints("0.0.0.0:1100")

  private val shutdownPromise = Promise[Done]
  private val db = "TESTDB"
  private val tableName = "OnlineRetailOrderDetail"
  private val cancelTableName = "OnlineRetailCancelDetail"

  /** Remove non-numerics from invoice numbers (C prefix used in cancellations) */
  def invoiceToLong(invoice: String): Long = invoice.replaceAll("[^\\d.]", "").toLong

  /** Convert mapped order details into a Row */
  def toRow(m: Map[String, String]): Row = Row(
    System.currentTimeMillis(), invoiceToLong(m("InvoiceNo")), m("StockCode"), m("Description"), m("Quantity").toInt,
    Timestamp.valueOf(m("InvoiceDate")), java.lang.Double.valueOf(m("UnitPrice")), m("CustomerID"), m("Country"))

  def websocket: Flow[Message, Message, Any] =
    Flow[Message].mapConcat {
      case textMessage: TextMessage =>
        println(ByteString(textMessage.getStrictText))

        textMessage.textStream
            .map(ByteString.fromString)
//        Source
//          .single(ByteString(textMessage.getStrictText))
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.withHeadersAsStrings(StandardCharsets.UTF_8, "InvoiceNo", "StockCode", "Description", "Quantity", "InvoiceDate", "UnitPrice", "CustomerID", "Country"))
          .map(x => toRow(x))
          .divertTo(EventStoreSink(db, cancelTableName), r => r.getInt(4) < 0)
          .runWith(EventStoreSink(db, tableName))
        TextMessage(
          Source
            .single("Sent text message data to Db2 Event Store")) :: Nil

      case binaryMessage: BinaryMessage =>
        println("BinaryMessage received")

        binaryMessage.dataStream
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMapAsStrings()) // First line is headers
          .map(x => toRow(x))
          .divertTo(EventStoreSink(db, cancelTableName), r => r.getInt(4) < 0)
          .runWith(EventStoreSink(db, tableName))
        TextMessage(
          Source
            .single("Sent binary message data to Db2 Event Store")) :: Nil
    }

  override protected def routes: Route =
    pathSingleSlash {
      complete {
        println("GET /")
        HttpEntity(
          ContentTypes.`text/html(UTF-8)`,
          "<html><body>OnlineRetailWebServer is running!</body></html>")
      }
    } ~
      pathPrefix("websocket") {
        path("orderitem") {
          println("Incoming ws: websocket/orderitem")
          handleWebSocketMessages(websocket)
        }
      }

  override protected def postHttpBindingFailure(cause: Throwable): Unit =
    println(s"postHttpBindingFailure: $cause")

  def start(host: String = "localhost", port: Int = 8080): Future[Done] = {
    val settings = ServerSettings(system.settings.config)
    Future {
      startServer(host, port, settings, system)
    }.map(_ => Done)
  }

  override protected def waitForShutdownSignal(system: ActorSystem)(implicit ec: ExecutionContext): Future[Done] =
    shutdownPromise.future

  def stop(): Future[Terminated] = {
    shutdownPromise.tryComplete(Success(Done))
    system.terminate()
  }

  def main(args: Array[String]): Unit = {
    println("Starting OnlineRetailWebServer...")
    start("localhost", 8080)
  }
}

object WebServer extends OnlineRetailWebServer

