package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class UvCount(windowEnd: Long, count: Long)

/**
  * UV：每个小时，访问网站的独立访客数
  * 只对一小时内的访客进行去重
  *
  * 结果形式：
  * 10:00~11:00 UV=5000
  * 11:00~12:00 UV=3000
  */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    //TODO 把整个窗口的数据拿下来， 进行窗口内的去重
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      //TODO dataStream，只能用WindowAll
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())

    processedStream.print()
    env.execute("unique visitor job")
  }
}

/**
  * 把整个窗口的数据拿出来，然后放到Set里面去重，Set里面有多少个元素，这个窗口期间的UV就有多少
  */
class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //TODO 使用全窗口函数apply，把同一个窗口的数据给拿出来，然后使用set进行去重，就得到了这个小时内部去重之后的UV。两个窗口之间不做去重，只进行内部去重。缺点：如果数据量大，都放在Set里面，也就是内存里面，容易OOM。
    //TODO 改进方法，如果数据量不大，直接使用Redis的Set去重，如果数据量大的话，使用Redis+布隆过滤器去重
    var idSet = Set[Long]()
    for (userBehavior <- input) {
      idSet += userBehavior.userId
    }
    //TODO 输出UV的统计结果
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}

