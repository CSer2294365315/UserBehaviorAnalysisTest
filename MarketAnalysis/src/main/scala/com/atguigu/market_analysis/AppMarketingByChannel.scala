package com.atguigu.market_analysis

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.market_analysis
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/25 16:35
  */

// 定义输入数据样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)
// 输出样例类
case class MarketViewCountByChannel(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)

/**
  * 造数据的，实际工作做测试也会用到
  */
class SimulatedDataSource() extends RichParallelSourceFunction[MarketUserBehavior]{
  // 是否运行的标识位
  var running = true
  // 定义出推广渠道和用户行为的集合
  val channelSet: Seq[String] = Seq("AppStore", "HuaweiStore", "XiaoStore", "weibo", "wechat")
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义一个随机数生成器
  val rand: Random = new Random()

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0L
    // 无限循环，生成随机数据
    while( running && count < maxElements ){
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val channel = channelSet(rand.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()
      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))
      count += 1
      Thread.sleep(10L)
    }
  }
}


/**
  * 对于统计窗口时间内，数量的，都是keyBy+window+process（Win）+ES
  * 对于统计窗口时间内，topN的，都是keyBy+window+process+ keyBy（windowEnd）+ process（定时器）
  *
  * TODO 需求： 各个渠道的引流效果，或者叫推广效果的动态变化
  * 求窗口时间内，从各个渠道来的各种操作的数量
  *
  * 动态变化，就是用窗口来实现的
  */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //TODO 对接日志采集通道
    val dataStream = env.addSource(new SimulatedDataSource())
      .assignAscendingTimestamps(_.timestamp)

    /**
      * TODO 统计渠道动态引流效果：常规套路，动态，肯定要开窗。不同渠道的引流效果，那就是分类查个数，分类查个数，就用keyBy+aggregate就可以了，光聚合还不够，还得能区分是哪个窗口的数据，所以还需要加上windowFunction。然后把流的数据直接输出到ES里面，在ES里面，写查询语句，按照窗口时间，一次查一批数据，就可以了
      */
    val processedStream = dataStream
      .filter(_.behavior != "UNINSTALL")
      .keyBy( data => (data.channel, data.behavior) )
      .timeWindow(Time.hours(1), Time.seconds(5))
    /**
      * TODO 使用keyBy+window+aggregate（AggregateFunction，WindowFunction）完成的，都可以用keyBy+window+process(ProcessWindowFunction来完成)
      */
      .process( new MarketCountByChannel() )

    processedStream.print()
    env.execute("app market job")
  }
}


/**
  * TODO 窗口内聚合，访问window时间戳
  */
class MarketCountByChannel() extends ProcessWindowFunction[MarketUserBehavior, MarketViewCountByChannel, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketViewCountByChannel]): Unit = {
    // 从上下文中获取window信息，包装成样例类
    val start = new Timestamp( context.window.getStart ).toString
    val end = new Timestamp( context.window.getEnd ).toString
    out.collect( MarketViewCountByChannel(start, end, key._1, key._2, elements.size) )
  }
}


