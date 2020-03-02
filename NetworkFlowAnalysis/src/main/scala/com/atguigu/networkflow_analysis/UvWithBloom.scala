package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.util.hashing.MurmurHash3


/**
  * 窗口内去重的UV
  * 使用布隆过滤器，对去重操作进行优化
  */
object UvWithBloom {
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

    //TODO 把整个窗口的数据拿下来，进行窗口内的去重
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .map(data => ("uv", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      //TODO 使用process功能强大，但是需要先把窗口数据缓存下来，然后才能触发process。如果窗口的数据量过大，那还没等触发process呢，就OOM了。所以，使用trigger对触发window操作的时机进行修改，默认是等到窗口结束在做聚合，现在改为每次来一个元素就进行聚合操作，这样，就把process改造成了增量聚合函数，每来一个元素，就进行聚合，提高了效率，避免了OOM
      .trigger(new MyTrigger())
      .process(new UvCountWithBlomm())

    processedStream.print()
    env.execute("unique visitor with bloom job")
  }
}

//TODO 自定义trigger，修改触发window操作的时间。为的是把process变成增量聚合函数
class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}


  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //TODO fire代表触发操作，purge代表清空缓存的
    TriggerResult.FIRE_AND_PURGE
  }
}




//TODO process，遍历窗口元素，用布隆过滤器在窗口内进行去重
class UvCountWithBlomm() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {

  //TODO 每次窗口，都会新建一个Redis连接，每个key一个窗口，不同的key，有不同的窗口，一个窗口时间段结束了，就开始下一个新的窗口
  lazy val jedis = new Jedis("localhost", 6379)
  //TODO 可以用富函数的open方法来在window刚创建的时候建立连接
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //TODO 每个窗口一个位图，位图存在Redis里面，就是个字符串。为了能够定位到不同窗口的位图，用k，v结构，v是字符串，key是window的时间戳就可以
    val storeKey = context.window.getEnd.toString
    //TODO 存储每个窗口有多少UV，就是最终的结果数据，不过需要一直更新，只要判断一个独立的UserID，就把这个值+1
    var count = 0L

    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong
    }

    val userId = elements.last._2.toString
    //TODO 计算某个UserID的hash值，也就是偏移量
    val offset = bloom.hash(userId, 61)


    //TODO 判断位图里面，这个UserId是否存在，用getBit方法来判断
    val isExist = jedis.getbit(storeKey, offset)
    if (!isExist) {
      // 如果不存在，那么将对应位置置1，然后count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    }
  }
}


//TODO 自定义hash规则，但一般不会自己实现，而是使用成熟的hash库来实现，比如MurMur
class Bloom(size: Long) extends Serializable {
  // 容量是2的 n次方
  private val cap = size

  def hash(value: String, seed: Int): Long = {
    var result = 0
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    // 返回hash，需要在cap范围内
    (cap - 1) & result
  }
}