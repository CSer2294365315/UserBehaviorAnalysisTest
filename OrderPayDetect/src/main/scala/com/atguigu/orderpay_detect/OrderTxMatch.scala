package com.atguigu.orderpay_detect

import com.atguigu.orderpay_detect.OrderTxMatch.getClass
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.orderpay_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/28 14:06
  */

// 输入数据样例类，OrderEvent用之前的
case class ReceiptEvent( txId: String, payChannel: String, eventTime: Long )

object OrderTxMatch {
  // 为了公用OutputTag，直接定义出来
  val unmatchedPays = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)



    // 读取数据，来自Order和Receipt两条流

    //TODO Order数据流
    val orderPayResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream = env.readTextFile(orderPayResource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[OrderEvent](Time.seconds(0)) {
        override def extractTimestamp(element: OrderEvent): Long = element.eventTime * 1000L
      })
      .filter( _.txId != "" )    // 过滤出txId不为空的订单支付事件
      .keyBy(_.txId)    // 用交易号分组进行两条流的匹配  //TODO 订单流--(交易号，订单日志）  第三方支付流水流--（交易号，流水日志）


    //TODO Receipt数据流
    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream = env.readTextFile(receiptResource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ReceiptEvent](Time.seconds(0)) {
        override def extractTimestamp(element: ReceiptEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.txId)    // 用交易号分组进行两条流的匹配

    // 合流并处理
    val processedStream = orderEventStream.connect(receiptEventStream)
      .process(new OrderTxPayMatch())

    processedStream.print("matched")
    processedStream.getSideOutput(unmatchedPays).print("unmatched pays")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatched receipts")
    env.execute("order tx match")
  }

  // 自定义实现一个CoProcessFunction
  class OrderTxPayMatch() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{


    // 用两个value state，来保存当前交易的支付事件和到账事件

    //TODO 值类型，订单事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("order-pay", classOf[OrderEvent]))
    //TODO 值类型 到账事件
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("tx-receipt", classOf[ReceiptEvent]))


    //TODO 客厅有两个椅子，外面有两个流。两个流，key相同的元素，会聚合到一起。这两个元素，一定有一个先到，一个后到。不管哪个先到，哪个后到，流程都一样。都是先看看对方的椅子（状态）上，有没有人，如果有人的话，那就生成一个输出，如果没有人，那我就坐在那等一会，同时设个闹钟，5秒钟后响。如果闹钟响的话，我就拍桌子走人（生成一条未匹配日志）。【】具体操作的话，就是在公共区域先设置两个状态。如果订单时间来了，那就触发订单事件流的方法，这个订单事件流的方法，先判断一下，支付事件，有没有到，判断有没有到的方法，是看一下支付事件的状态是不是为空。因为如果支付事件到了，而我订单时间没到，那支付事件肯定存到状态里面了。所以，只要支付事件到了，就一定在支付事件的状态里面。同时如果支付事件的状态为空，那就一定说明支付事件还没有到。如果支付事件的状态为空的话，那说明支付事件还没到，那订单事件就把自己存到公共区域的状态里面，等后期支付之间到了之后，支付时间会负责把订单事件取出来，然后结合，输出出去。【】主要注意的是，connect，是把key相同的进行connect。而不是像齿轮一样join。key相同的两条流中的元素，才会connect

    //TODO 我是Order
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {

      //订单支付事件来了，需要考虑是否有对应的到账事件
      //TODO 状态里面有到账事件吗
      val receipt = receiptState.value()
      if(receipt != null){  //TODO 如果有到账事件，输出（订单事件，到账事件）到主流，清空存储的到账事件（我是订单事件）
        // 如果已经有对应的receipt，匹配输出到主流
        out.collect((pay, receipt))
        receiptState.clear()
      } else{  //TODO 如果我订单事件已经到了，但是另一条流的到账事件没有到，那就把我订单事件给存到公共状态区，对到账流可见，去那里等着它
        // 如果receipt还没来，存储pay状态，注册定时器等待
        payState.update(pay)
        //TODO 如果5秒内receipt来了呢，这个定时器是怎么被注销的？其实这个定时器是不会被取消的，只要是设置了定时器，就一定会执行，只不过，在定时器里面进行了判断，虽然执行了定时器，但是定时器什么都没有执行
        ctx.timerService().registerEventTimeTimer( pay.eventTime * 1000L + 5000L )    // 等待5秒，receipt还不来就输出报警
      }
    }

    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 到账事件来了，需要考虑是否有对应的支付事件
      val pay = payState.value()
      if(pay != null){
        // 如果已经有对应的 pay，匹配输出到主流
        out.collect((pay, receipt))
        payState.clear()
      } else{
        // 如果 pay还没来，存储 receipt状态，注册定时器等待
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer( receipt.eventTime * 1000L + 3000L )    // 等待3秒，pay还不来就输出报警
      }
    }

    //TODO 定时器响，拍桌子。输出错误日志。
    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      //TODO 定时器就这一个方法，无论是谁定义的定时器，都是调用这个相同的方法。如果希望不同人调用的这个方法执行不同的逻辑，那就在这个方法内部作区分。
      // 比如本案例，如果是订单表等的不耐烦了，也就是说支付事件状态为空，那就输出一条报警信息，说支付事件超时了。
      if( payState.value() != null ){
        // pay有值，说明receipt没到
        ctx.output(unmatchedPays, payState.value())
      } else if( receiptState.value() != null ){  //TODO 如果是支付事件来了很久，等的不耐烦了， 那就输出一条报警信息，说订单事件超时了
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      //TODO 不管支付事件来没来，只要设了闹钟，就一定会执行onTimer，但是再执行oTimer的时候，发现支付事件状态不为空，也就是在5秒钟内已经来了，那就什么不生成任何报警信息就可以了
      payState.clear()
      receiptState.clear()
    }
  }
}

