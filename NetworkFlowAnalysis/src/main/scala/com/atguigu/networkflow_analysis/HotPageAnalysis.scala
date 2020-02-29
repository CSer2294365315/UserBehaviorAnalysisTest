package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.networkflow_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 9:23
  */
// 定义一个输入数据的样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 定义中间的聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

/**
  * 实时热门页面分析
  * 每隔5分钟输出一次，最近一小时内，用户访问量前N的URL
  */
object HotPageAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)


    val resource = getClass.getResource("/apache.log")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(" ")
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3)).getTime
        ApacheLogEvent(dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6))
      })

      //TODO 处理乱序数据的第一重保障：延迟
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })


    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(10))
      //TODO 处理乱序数据的第二种保障，允许迟到数据。window暂时不关闭，每来一条迟到的数据，就把这条迟到数据，合并到之前窗口的计算结果里面去
      .allowedLateness(Time.minutes(1))
      //TODO 处理乱序数据的第三重保障，晚于最大迟到时间的数据，不要丢弃，而是输出到侧输出流里面，lambda架构，找机会修正之前的实时统计结果，会让统计更加准确
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      //TODO  和之前TopN一样，窗口内聚合，求窗口内每个URL的点击量。加上所属于哪个window
      .aggregate(new UrlCountAgg(), new UrlCountResult())


    val resultStream = aggStream
      //TODO 通过keyBy把同一窗口的数据聚合在一起，这样才能对同一窗口的URL进行排序。
      .keyBy(_.windowEnd)
      //TODO 求相同窗口的数据的TopN，主要是利用process设置定时器，在每次窗口结束的时候，都把统一窗口的数据排一下序，计算一下TopN，然后输出
      .process(new TopNHotUrls(5))

    resultStream.print("result")
    //TODO 侧输出流的数据，可以用来修正实时计算结果，lambda架构
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    env.execute("hot page job")
  }
}


/**
  * aggregate，对同一个URL的点击量进行统计
  */
class UrlCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
  * aggregate（聚合函数，windowFunction），利用windowFunction，给聚合之后的结果加上窗口的时间戳，这样后续就能区分这条数据是属于哪个窗口的了，就能把同一窗口的数据给聚合起来，然后进行窗口内的排序。否则无法进行窗口内的排序
  */
class UrlCountResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}


/**
  * keyBy之后，同一窗口的URL的点击量统计结果就聚合在一起了，需要通过定时器，在每次窗口结束的时候，都调用定时器的回调方法，把同一窗口的数据拉取下来，然后进行排序，输出
  */
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
  //TODO 如果不考虑迟到的那部分数据的话，就把每个窗口的数据，放在List里面就可以，每次窗口结束，都触发定时器的回调方法求TopN就可以了。如果考虑迟到数据的话，因为每条迟到数据都会触发窗口后续的所有计算，而且所有计算，都是在原来计算的基础上进行计算的。所以，肯定会产生数据重复的问题，比如原来整个窗口，在进行完聚合之后，会输出一批URL的点击量统计数据。但是来了一条迟到数据，那就会重新把这个窗口内的每个URL的点击量给统计一遍，然后再重新把这个窗口的URL点击量的统计数据输出一遍。这样后面的keyBy之类的计算，就会收集到同一个窗口的两批数据。如果还是正常的keyBy，然后统计的话，就错了，比如同一个URL的点击量可能出现两次，或者多次。解决这个问题的办法，就是不用List了，换成用Map，每次迟到数据来了，会把同一个窗口的URL的点击量数据重新发一遍，然后经过keyBy之后，到达process，process会用新来的一批数据，覆盖掉旧的数据，然后重新触发定时器的回调方法，进行TopN的统计。
  lazy val urlMapState: MapState[String, Long] = getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("url-map", classOf[String], classOf[Long]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    //TODO 来一条数据（URL，次数），就把这个存到List里面。经过改进之后，就是存到Map里面
    urlMapState.put(value.url, value.count)
    //TODO 设置定时器。不是为数据设置定时器，而是借用数据里面的窗口结束时间戳，给窗口设置定时器。等到窗口结束的时候，进行统计计算和输出
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }


  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    val allUrlsCount: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    val iter = urlMapState.entries().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      //TODO 把Map状态中存的URL统计数据，提取出来，方便排序。不然在Map里面不方便排序
      allUrlsCount += ((entry.getKey, entry.getValue))
    }


    //TODO 排序，取TopN
    val sortedUrlsCount = allUrlsCount.sortWith(_._2 > _._2).take(topSize)

    //TODO 拼接字符串，把字符串传给流，让流进行输出
    val result: StringBuilder = new StringBuilder()
    result.append("==================================\n")
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环遍历sortedList，输出前三名的所有信息
    for (i <- sortedUrlsCount.indices) {
      val currentUrl = sortedUrlsCount(i)
      result.append("No").append(i + 1).append(":")
        .append(" URL=").append(currentUrl._1)
        .append(" 访问量=").append(currentUrl._2)
        .append("\n")
    }

    Thread.sleep(1000L)
    //TODO 把拼接好的结果字符串，传到流中，让流输出数据，而不是在这里输出数据。如果是ES的话，可以在这里输出数据
    out.collect(result.toString())
  }
}