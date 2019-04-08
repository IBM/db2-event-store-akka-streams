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
import akka.http.scaladsl.model.ContentTypes
import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.ws.BinaryMessage
import akka.http.scaladsl.model.ws.Message
import akka.http.scaladsl.model.ws.TextMessage
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.alpakka.csv.scaladsl.CsvToMap
import akka.stream.alpakka.ibm.eventstore.scaladsl.EventStoreSink
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.util.ByteString
import com.ibm.event.common.ConfigurationReader
import org.apache.spark.sql.Row

import scala.concurrent.Future

object OnlineRetailWebServer extends HttpApp {

  private val DB = "TESTDB"
  private val TableName = "OnlineRetailOrderDetail"
  private val CancelTableName = "OnlineRetailCancelDetail"

  /** Remove non-numerics from invoice numbers (C prefix used in cancellations) */
  private def invoiceToLong(invoice: String): Long =
    invoice.replaceAll("[^\\d.]", "").toLong

  /** Convert mapped order details into a Row */
  private def toRow(m: Map[String, String]): Row =
    Row(
      System.currentTimeMillis(),
      invoiceToLong(m("InvoiceNo")),
      m("StockCode"),
      m("Description"),
      m("Quantity").toInt,
      Timestamp.valueOf(m("InvoiceDate")),
      java.lang.Double.valueOf(m("UnitPrice")),
      m("CustomerID"),
      m("Country"))

  def websocket(implicit materializer: Materializer): Flow[Message, Message, Any] = {
    implicit val executionContext = materializer.executionContext
    val writerSink: Sink[Map[String, String], Future[Done]] =
      Flow[Map[String, String]]
        .map(x => toRow(x))
        .divertTo(EventStoreSink(DB, CancelTableName), r => r.getInt(4) < 0)
        .toMat(EventStoreSink(DB, TableName))(Keep.right)

    Flow[Message].mapAsync(1) {
      case textMessage: TextMessage =>
        textMessage.textStream
          .map(ByteString.fromString)
          .via(CsvParsing.lineScanner())
          .via(
            CsvToMap.withHeadersAsStrings(
              StandardCharsets.UTF_8,
              "InvoiceNo",
              "StockCode",
              "Description",
              "Quantity",
              "InvoiceDate",
              "UnitPrice",
              "CustomerID",
              "Country"))
          .runWith(writerSink)
          .map(_ => TextMessage("Sent text message data to Db2 Event Store"))

      case binaryMessage: BinaryMessage =>
        binaryMessage.dataStream
          .via(CsvParsing.lineScanner())
          .via(CsvToMap.toMapAsStrings()) // First line is headers
          .runWith(writerSink)
          .map(_ => TextMessage("Sent binary message data to Db2 Event Store"))
    }
  }

  def routes: Route =
    extractLog { logger =>
      concat(
        pathSingleSlash {
          logger.info("GET /")
          complete(HttpEntity(
            ContentTypes.`text/html(UTF-8)`,
            "<html><body>OnlineRetailWebServer is running!</body></html>"))
        },
        path("websocket" / "orderitem") {
          logger.info("Incoming ws: websocket/orderitem")
          extractMaterializer { materializer =>
            handleWebSocketMessages(websocket(materializer))
          }
        })
    }

  def main(args: Array[String]): Unit = {
    ConfigurationReader.setConnectionEndpoints("0.0.0.0:1100")
    println("Starting OnlineRetailWebServer...")
    startServer("localhost", 8080)
  }
}
