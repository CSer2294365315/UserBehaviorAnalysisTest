package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义中间聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


//就这三种可能
//keyBy，window，process
//keyBy，process。
//window，process


/**
  * 实时热门商品
  * 每隔5分钟，输出一次最近1小时内，
  */
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    /**
      * 从Kafka读数据
      */
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))
      //TODO String日志，映射成日志对象
      .map(data => {
      val dataArray = data.split(",")
      UserBehavior(dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
    })
      //TODO 时间戳是升序的，所以用AscendingTimeStamp
      .assignAscendingTimestamps(_.timestamp * 1000L)

    /**
      * 按照商品ID分组，开窗，组内聚合求个数，输出出去
      * 加上时间戳，因为后面的聚合要用
      */
    val aggregatedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.hours(1), Time.minutes(5))
      //      .process()
      //TODO 如果仅仅是想要聚合的话，用AggravateFunction够了
      .aggregate(new CountAgg(), new WindowResult())

    /**
      * 通过keyBy，把同一窗口的数据聚到同一个组，然后再用process把整个组的数据拿出来，做一下排序，TopN，输出
      * 输出，其实是需要写入到ES的
      */
    val resultStream = aggregatedStream
      .keyBy("windowEnd") // 按照窗口分组
      .process(new TopNItems(3)) // 自定义 process function做排序输出

    /**
      * 输出效果：每隔5分钟，刷新一次的淘宝大屏榜单
      * windowEnd
      * top1，XXX商品，点击量
      * top2，XXX商品，点击量
      */
    resultStream.print()
    env.execute()
  }
}

/**
  * 统计每个商品Id，对应的日志有多少个的Aggregate聚合函数
  */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  // 每来一条数据就加 1
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}


/**
  * 用在aggregate（聚合，window）里面，在聚合函数完成聚合之后，再把数据输入到WindowFunction里面，加上window的时间范围，
  * 方便后续把同一个窗口的各个商品id的点击量统计数据聚合到一起
  */
class WindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd: Long = window.getEnd
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

/**
  * 之前的数据，已经做好了那个窗口，哪个商品ID，对应的点击量是多少的统计了。
  * 还需要把同一窗口的各个商品点击量的数据聚合到一起，然后把同一窗口的数据给拉取下来，做一下排序，取TopN，然后输出出去就可以了
  */
class TopNItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  private var itemListState: ListState[ItemViewCount] = _

  /**
    * KeyBy后面的KeyedProcessFunction，不是增量聚合函数。也是没来一条数据就处理一条数据，和aggregate是一样的，所以效率也很高。
    * 在遍历key中的每一个元素的时候，把都把这个元素给存到状态里面，这样才能把所有数据聚到一起进行以下排序。
    * 同时，在遍历数据的时候，需要根据数据中的窗口时间，设置上窗口结束的时候的定时器。多个相同时间的定时器，只会注册一个。
    * 等到窗口时间结束的时候，就会调用定时器的回调用法，回调方法会把存在状态里面的元素数据给排一下序，topN，然后输出
    * 这样就能实现，每次窗口结束，都进行计算，同时输出本窗口内的所有数据了
    */
  override def open(parameters: Configuration): Unit = {
    itemListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-list", classOf[ItemViewCount]))
  }


  /**
    * key的每个元素调用一次这个方法。如果需要把窗口的所有元素取出来，需要使用状态
    */
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }


  //到达窗口结束时间之后，定时器响，调用回调方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    import scala.collection.JavaConversions._

    /**
      * 排序
      */
    val allItemsList = itemListState.get().iterator().toList
    val sortedItemsList = allItemsList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    /**
      * 把排序的结果输出，可以写成ES的存储逻辑
      */
    val result: StringBuilder = new StringBuilder()
    result.append("==================================\n")
    result.append("窗口关闭时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 用一个for循环遍历sortedList，输出前三名的所有信息
    for (i <- sortedItemsList.indices) {
      val currentItem = sortedItemsList(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }

    Thread.sleep(1000L)

    //可以把想要输出的数据写到流里面，然后由流进行输出，这里不进行输出
    out.collect(result.toString())

    // 清空list state，释放资源
    itemListState.clear()
  }
}