package stock

/**
 *  1. For every 1 minute, we need to calculate:
 *      a. Maximum trade price
 *      b. Minimum trade price
 *      c. Maximum trade volume
 *      d. Minimum trade volume
 *      e. %age change in Max Trade Price from previous 1 minute
 *      f. %age change in Max Trade Volume from previous 1 minute
 *
 *  2. For every 2 minutes window, raise an alert if Max Trade Price is changed (up or down) by more then 5% (threshold
 * value) and capture that record.
 */

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import java.text.SimpleDateFormat
import java.sql.Timestamp

import stock.helper.{TrackAlert, TrackChange}

//Pass the output path of file in Program Argument

object StockAnalysis {

  val thresholdValue =  5
  val host = "localhost"
  val portNumber = 9090

  def main(args: Array[String]): Unit = {

    if(args.length == 0) {
      println("Output file path not found!!")
      return
    }

    val outputFilePath = args(0)

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    println("**** STOCK ANALYSIS STARTED ****")

    //Run DataServer
    val text: DataStream[String] = env.socketTextStream(host, portNumber)

    //Date, Timestamp, CompanyName, StockPrice, StockVolume
    val data: DataStream[(String, String, String, Double, Int)] = text.map(_.split(","))
      .map(fields => (fields(0), fields(1), fields(2), fields(3).toDouble, fields(4).toInt))

    val mapped: DataStream[(String, String, String, Double, Int)] =
      data
        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor[(String, String, String, Double, Int)] {
          final val sdf: SimpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")

          override def extractAscendingTimestamp(value: (String, String, String, Double, Int)): Long = {
            try {
              val ts: Timestamp = new Timestamp(sdf.parse(value._1 + " " + value._2).getTime)
              ts.getTime
            } catch {
              case e: Exception => throw new java.lang.RuntimeException(s"Parsing Error!! ${e.getMessage}")
            }
          }
        })

    //Compute per window statistics
    val changeDS: DataStream[String] = mapped.keyBy(_._3)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .process(new TrackChange())

    changeDS.writeAsText(outputFilePath + "/report.txt").setParallelism(1)

    //Alert when price changed from one window to another is more than Threshold
    val alertDS: DataStream[String] = mapped.keyBy(_._3)
      .window(TumblingEventTimeWindows.of(Time.minutes(2)))
      .process(new TrackAlert(thresholdValue))

    alertDS.writeAsText(outputFilePath + "/alarm.txt").setParallelism(1)

    env.execute("Stock Analysis")

    println("**** STOCK ANALYSIS ENDED ****")
  }
}