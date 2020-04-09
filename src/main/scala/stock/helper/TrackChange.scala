package stock.helper

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TrackChange extends ProcessWindowFunction[(String, String, String, Double, Int), String, String, TimeWindow] {

  var prevWindowMaxTrade: ValueState[Double] = _
  var prevWindowMaxVolume: ValueState[Int] = _

  override def process(key: String, context: Context, input: Iterable[(String, String, String, Double, Int)], out: Collector[String]): Unit = {
    var windowStart: String = ""
    var windowEnd: String = ""
    var windowMaxTrade: Double = 0.0
    var windowMinTrade: Double = 0.0
    var windowMaxVol: Int = 0
    var windowMinVol: Int = 0
    var companyName: String = ""

    for ((date, time, name, price, volume) <- input) {
      companyName = name;

      if (windowStart.isEmpty) {
        windowStart = date + ":" + time
        windowMinTrade = price
        windowMinVol = volume
      }
      if (price > windowMaxTrade) {
        windowMaxTrade = price
      }

      if (price < windowMinTrade) {
        windowMinTrade = price
      }

      if (volume > windowMaxVol) {
        windowMaxVol = volume
      }

      if (volume < windowMinVol) {
        windowMinVol = volume
      }

      windowEnd = date + ":" + time
    }

    var maxTradeChange: Double = 0.0
    var maxVolChange: Double = 0.0

    if (prevWindowMaxTrade.value() != 0) {
      maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) * 1.0 / prevWindowMaxTrade.value()) * 100;
    }

    if (prevWindowMaxVolume.value() != 0) {
      maxVolChange = ((windowMaxVol - prevWindowMaxVolume.value()) * 1.0 / prevWindowMaxVolume.value()) * 100;
    }

    out.collect(companyName + " : " + windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " +
      maxTradeChange + "," + windowMaxVol + ", " + windowMinVol + ", " + maxVolChange)

    prevWindowMaxTrade.update(windowMaxTrade)
    prevWindowMaxVolume.update(windowMaxVol)
  }

  override def open(parameters: Configuration): Unit = {
    val maxTradeDescriptor = new ValueStateDescriptor[Double]("prevWindowMaxTrade",
      TypeInformation.of(new TypeHint[Double] {}), 0.0)
    prevWindowMaxTrade = getRuntimeContext.getState(maxTradeDescriptor)

    val maxVolDescriptor = new ValueStateDescriptor[Int]("prevWindowMaxVolume",
      TypeInformation.of(new TypeHint[Int] {}), 0)
    prevWindowMaxVolume = getRuntimeContext.getState(maxVolDescriptor)
  }

}
