package com.atguigu.orderpay_detect

import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/28 10:35
  */

// 输入数据的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)
// 输出订单检测结果样例类
case class OrderResult(orderId: Long, resultMsg: String)


/**
  * TODO 订单支付情况实时监控（刷单行为实时监控）
  */
object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 0. 从文件中读取数据
    val resource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.orderId)

    //TODO 使用CEP进行模式匹配，要求创建订单后，15分钟内完成支付。具体实现的话，就是首先匹配到创建订单的事件，创建订单事件后面非严格紧邻一个支付事件，在15分钟内完成匹配
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //TODO 把这个模式，应用到流上，把流里面匹配上的事件给输出到一个新的流里面去，PatternStream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    // 3. 定义一个侧输出流标签，用于把 timeout事件输出到侧输出流里去
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    //TODO 正确时间内匹配上的事件串，存在主流里面。超时匹配上的事件串，存在侧输出流里面，比如对于先创建了订单，15分钟后进行了支付，那么这个创建订单的事件和支付的事件，组成的事件串，存在侧输出流中。如果想要这部分超时事件的话，到侧输出流里面去获取
    val resultStream = patternStream.select( orderTimeoutOutputTag,
      new OrderTimeoutSelect(),
      new OrderPaySelect())

    resultStream.print("payed")
    //TODO 超时匹配上的，到侧输出流里面获取
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    env.execute("order timeout detect job")
  }
}


//TODO 从流中，匹配到超时的事件串
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], timeoutTimestamp: Long): OrderResult = {
    val timeoutOrderId = pattern.get("create").iterator().next().orderId
    OrderResult( timeoutOrderId, "timeout at " + timeoutTimestamp )
  }
}

//TODO 从流中，抓取在规定时间内匹配上的事件串
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId = pattern.get("pay").iterator().next().orderId
    OrderResult( payedOrderId, "payed successfully" )
  }
}