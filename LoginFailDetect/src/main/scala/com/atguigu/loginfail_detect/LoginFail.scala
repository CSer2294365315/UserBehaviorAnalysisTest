package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: UserBehaviorAnalysis
  * Package: com.atguigu.loginfail_detect
  * Version: 1.0
  *
  * Created by wushengran on 2020/2/26 11:47
  */

// 输入登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, msg: String)

/**
  * TODO 恶意登录监控
  * 2秒内，发生两次连续的登录失败，就判定为恶意登录
  */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })


    //TODO keyBy的过程类似于停车，不同的key，停在不同的道上，相同的key的车，停在一起
    //TODO 如果想对keyBy后的数据，做复杂的条件处理的话，那就用process，普通的累加，就用aggregate
    //TODO 按照UserId分组，对每个UserId单独的判断
    val warningStream = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarning(2))

    warningStream.print()
    env.execute("login fail detect job")
  }
}



class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  //TODO 保存上次是否登录失败的list状态
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(
    new ListStateDescriptor[LoginEvent]("saved-loginfails", classOf[LoginEvent]))

  /**
    * TODO 恶意登陆的标准：2秒内，连续两次登陆失败，那就算是恶意登录
    * TODO 实现方法：为每个key设置一个list状态，然后这个key来一条数据，就判断一下，看看是不是登录失败，如果是登陆失败，那就往这个list里面插入一条失败日志。如果是登陆成功，那说明这次肯定不会形成连续两次登陆失败了，也就是不能判定为恶意登录了，那list里面那条数据留着就没什么用了，还干扰分析，所以就把list里面的数据删掉就可以了。这里面需要注意一下， 就是如果是登陆失败的话，还需要判断下list里面是否已经有登录失败的日志了，如果有的话，判断一下这两个登录失败日志的时间差，如果相差小于2秒，那就可以说明，2秒内，发生了连续两次登陆失败，就可以判定为恶意登录了。
    */
  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {

    //TODO 如果登录失败的话，有两种情况，一种，是上一次没有登陆失败，也就是list里面没有登录失败日志，那这次就是首次登陆失败，就把这条登录失败的信息加入到list里面。还有一种情况，就是上一次也登陆失败了，那就判断一下，看是否两次登陆失败是在2秒内发生的，也就是这两次登陆失败的时间间隔是不是在2秒内，如果是的话，那就是恶意登录了
    if (value.eventType == "fail") {
      val iter = loginFailListState.get().iterator()
      if (iter.hasNext) {  //TODO 看看是否上一次也登陆失败了
        val firstFailEvent = iter.next()
        if ((firstFailEvent.eventTime - value.eventTime).abs < 2) {  //TODO 如果上一次也登陆失败的话，而且两次登录失败在2秒内，那就报警
          out.collect(Warning(value.userId, firstFailEvent.eventTime, value.eventTime, "login fail for 2 times"))
        }
        //TODO 如果上一次也登陆失败了，但是不是在2秒内登录失败的，那就不算恶意登录，就把list清空，要不然没法判断上次是不是登录失败了
        if (value.eventTime > firstFailEvent.eventTime) {
          loginFailListState.clear()
          loginFailListState.add(value)
        }
      } else {
        //TODO 如果上一次没有登陆失败，那就把这次登陆失败的信息，存到list里面
        loginFailListState.add(value)
      }
    } else {
      //TODO 如果这次成功登陆了，那肯定不满足连续两次登陆失败的条件了，那就清空list，否则不好判断上次是否登录失败了
      loginFailListState.clear()
    }
  }

  //  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
  //    import scala.collection.JavaConversions._
  //    // 先从liststate中取出所有的登录失败事件
  //    val allLoginFails = loginFailListState.get().iterator().toList
  //    // 判断总共的失败事件是否超过 maxFailTimes
  //    if( allLoginFails.length >= maxFailTimes ){
  //      // 输出报警信息
  //      out.collect( Warning(allLoginFails.head.userId,
  //        allLoginFails.head.eventTime,
  //        allLoginFails.last.eventTime,
  //        "login fail in 2s for " + allLoginFails.length + " times") )
  //    }
  //    // 清空状态
  //    loginFailListState.clear()
  //  }
}