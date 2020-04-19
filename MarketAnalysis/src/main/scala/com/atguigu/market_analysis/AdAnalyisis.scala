package com.atguigu.market_analysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.{AggregateFunction, RichFilterFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector



// 输入log数据样例类
case class AdClickLog(userId: Long, adId: Long, province: String, city: String, timestamp: Long)

// 输出按省份分类统计结果样例类
case class AdCountByProvince(windowEnd: String, province: String, count: Long)

// 定义侧输出流黑名单报警信息的样例类
case class BlackListWarning(userId: Long, adId: Long, msg: String)

/**
  * 统计各省每小时的点击量，对于恶意点击的部分，需要加入到黑名单
  * 重点是识别恶意点击，同时加入黑名单
  *
  * 核心：使用使用侧输出流的方式实现过滤，侧输出流对接的就是黑名单
  */
object AdAnalyisis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val resource = getClass.getResource("/AdClickLog.csv")
    val adLogStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        AdClickLog(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    /**
     * 过滤（分流）
     */
    val filterBlackListStream = adLogStream
      .keyBy(data => (data.userId, data.adId))
      //TODO process 只要没开窗，就不会等到整个窗口数据都到齐了才处理，而是来一条数据就处理一条数据（KeyedProcessFunction）。如果开窗了，也就是ProcessWindowFunction，那就会等窗口的数据都到齐了再处理。
      //TODO keyedProcessFunction，一条一条处理，定时器，侧输出流。ProcessWindowFunction，一个窗口一个窗口的处理，window信息，侧输出流
      .process(new FilterBlackListUser(100))


    /**
      * 统计每个省，每个小时的点击量
      *
      * 统计窗口内数量：keyBy+window+aggregate（统计窗口内个数）
      * 统计窗口内topN：keyBy+window+aggregate（统计窗口内个数） + keyBy+process（排序，topN）
      */
    //TODO 把黑客攻击数据过滤出去了，同时完成了对脏数据的统计，主流里面剩下的都是好的数据了。但是我不禁想过滤掉脏数据，还想统计一下各个省份每小时的点击量各是多少。那就把主流，也就是原来的流，按照省份keyBy，然后用aggregate聚合一下，在通过WindowFunction加上窗口信息，然后存到ES里面，再进一步的按照windowEnd时间统计一下就可以了
    val adCountStream = filterBlackListStream
      .keyBy(_.province)
      .timeWindow(Time.hours(1), Time.seconds(5))
      .aggregate(new AdCountAgg(), new AdCountResult())
    //TODO 输出形式：
    // (10：00~11:00，北京，10000)，(10：00~11:00，上海，20000)，
    // (11：00~12:00，北京，10000)，(11：00~12:00，上海，20000)，甚至10:00~11:00和11:00~12:00的数据式混合在一起的，没关系，使用ES，按照时间在统计一下就可以了。不用再按照windowEnd KeyBy和process了，那个是求TopN的。如果不求topN，keyBy+window+aggregate(Aggregate,windowFunction)就能够实现窗口内各个key的数据的统计了

    adCountStream.print("count")
    filterBlackListStream.getSideOutput(new OutputTag[BlackListWarning]("blacklist")).print("black-list")
    env.execute("ad analysis job")
  }
}

//TODO 统计窗口内个数
class AdCountAgg() extends AggregateFunction[AdClickLog, Long, Long] {
  override def add(value: AdClickLog, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//TODO 加上窗口信息
class AdCountResult() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    val end = new Timestamp(window.getEnd).toString
    out.collect(AdCountByProvince(end, key, input.iterator.next()))
  }
}

/**
  * 遍历每个元素，判断是否需要分流到黑名单：
  * 通过主流和侧输出流，实现对原来的流的过滤。符合条件的继续转发到主流里面，不符合条件的，转发到侧输出流，或者干脆哪里都不转发，就是过滤了。
  * 可以理解为过滤，可以理解为分流
  *
  * @param maxCount
  */
class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog] {

  //TODO 状态 每个key一个状态，用于保存这个用户，对这个商品的点击量的，定时清除
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

  //TODO 状态，记录这个用户对这个广告的点击，是否已经加入到黑名单了，防止重复加入黑名单，加入一次就可以了
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))

  lazy val resetTimerState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timer", classOf[Long]))

  override def processElement(value: AdClickLog, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, out: Collector[AdClickLog]): Unit = {

    //TODO keyedProcessFunction的状态，是针对于每个key的，因为没有窗口，所以，只要这个流在，那这个状态会一直保持，程序跑多久，状态就保持多久，除非自己清除状态。要想使用在流中能跨数据访问的状态，就必须使用提供的方法获得的状态。而不能是自己定义的java变量
    val curCount = countState.value()

    //TODO 用户每天不能点击同一个广告超过100次，仅仅是每天不能超过100次，所以当天统计的状态，第二天需要删除掉，否则第二天的统计的点击量，就接着上一天的统计量来了，这是不对的
    if (curCount == 0) {
      val ts = (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) + 1) * (1000 * 60 * 60 * 24)
      //TODO keyBy之后，会分为很多key，每个key都有状态，从业务逻辑的角度来讲，只统计每天的点击量，所以第二天需要把这个key的状态清除掉
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerState.update(ts)
    }

    //TODO 如果这个用户对这个广告的点击量已经超过阈值了，而且没有发送过黑名单，那么就往侧输出流输出报警日志，也就是拉入黑名单
    if (curCount >= maxCount && isSentState.value() == false) {
      ctx.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "Click over " + maxCount + " times"))
      //TODO 状态+1
      countState.update(curCount + 1)
      //TODO 以前没拉入过黑名单，这次拉入黑名单了，那以后就不要拉黑名单了
      isSentState.update(true)
      //TODO 点击量超过阈值了，但以前已经拉过黑名单了，那既不输出到主流，也不输出到侧输出流，相当于这条数据丢弃了，也就是从主流中过滤掉了。这也就是用process实现过滤功能。通过判断是否满足条件，如果满足，那就继续输出到主流里面，如果不满足，那就输出到侧输出流，或者干脆不输出到侧输出流，相当于过滤掉了
    } else if (curCount >= maxCount && isSentState.value() == true) {
      //TODO 状态+1
      countState.update(curCount + 1)
      //TODO 如果点击量没超过阈值，那就把这条日志继续转发到主流当中，同时转态+1
    } else {
      //TODO 状态+1
      countState.update(curCount + 1)
      //TODO 数据原封不动的输出到主流
      out.collect(value)
    }
  }

  //TODO 每个key，每晚会响一次，来把状态清除掉
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    if (timestamp == resetTimerState.value()) {
      isSentState.clear()
      countState.clear()
      resetTimerState.clear()
    }
  }
}

/**
  * 拉入黑名单，其实就是分流操作。之所以使用process来做分流，而不用filter，是因为filter不能输出到侧输出流，使用富函数也不行，而process可以，所以就用process，而不用filter
  */
class MyFilter(maxCount: Int) extends RichFilterFunction[AdClickLog] {
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))

  override def filter(value: AdClickLog): Boolean = {
    val curCount = countState.value()
    if (curCount >= maxCount) {
      false
    } else {
      countState.update(curCount + 1)
      true
    }
  }

  override def close(): Unit = {
    countState.clear()
  }
}