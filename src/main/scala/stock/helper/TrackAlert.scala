package stock.helper

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TrackAlert(threshold: Int) extends ProcessWindowFunction[(String, String, String, Double, Int), String, String, TimeWindow] {

  var prevWindowMaxTrade: ValueState[Double] = _

  override def process(key: String, context: Context, input: Iterable[(String, String, String, Double, Int)], out: Collector[String]): Unit = {
    var prevMaxTrade: Double = prevWindowMaxTrade.value()
    var currMaxTrade: Double = 0.0
    var currMaxTimestamp: String = ""
    var companyName: String = ""

    for ((date, time, name, price, volume) <- input) {
      companyName = name;
      if (price > currMaxTrade) {
        currMaxTrade = price
        currMaxTimestamp = date + ":" + time
      }
    }

    //Check if change is more than specified threshold
    val maxTradePriceChange: Double = ((currMaxTrade - prevMaxTrade) / prevMaxTrade) * 100

    if(prevMaxTrade != 0 &&   //don't calculate delta for the first time
      Math.abs((currMaxTrade - prevMaxTrade) / prevMaxTrade) * 100 > this.threshold) {
      out.collect(companyName + " : Large Change Detected of : " + maxTradePriceChange + "%" + " (" + prevMaxTrade + " - " + currMaxTrade + ") at " + currMaxTimestamp)
    }

    prevWindowMaxTrade.update(currMaxTrade)
  }

  override def open(parameters: Configuration): Unit = {
    val maxTradeDescriptor = new ValueStateDescriptor[Double]("prevWindowMaxTrade",
      TypeInformation.of(new TypeHint[Double] {}), 0.0)
    prevWindowMaxTrade = getRuntimeContext.getState(maxTradeDescriptor)
  }

}