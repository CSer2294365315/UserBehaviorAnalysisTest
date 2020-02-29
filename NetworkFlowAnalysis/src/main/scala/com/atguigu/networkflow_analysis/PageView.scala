package com.atguigu.networkflow_analysis

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 14:06
  */

// 定义样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
case class PvCount(windowEnd: Long, count: Long)

/**
  * 求某个页面的每小时浏览量
  * 每小时更新一次，每次显示上一小时，某个页面的总访问量
  * 结果形式：
  * 10:00~11:00，page1访问量==10000
  * 11:00~12:00，page1访问量==20000
  */
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)


    val processedStream = dataStream
      .filter(_.behavior == "pv")
      //TODO 为了把所有日志数据，转成无差别的（“pv”，1）类型，一方面方便keyBy，另一方面可以用聚合的方法求个数
      .map( data => ("pv", 1) )
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //TODO 整个窗口的数据，只有一个key，只出一条聚合后的数据。不像topN,有多种key，出多种数据
      .aggregate(new PvCountAgg(), new PvResult())

    processedStream.print()
    env.execute("page view job")
  }
}

//TODO 查数的
class PvCountAgg() extends AggregateFunction[(String, Int), Long, Long]{
  override def add(value: (String, Int), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//TODO 加上窗口时间戳信息
class PvResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect( PvCount(window.getEnd, input.iterator.next()) )
  }
}
