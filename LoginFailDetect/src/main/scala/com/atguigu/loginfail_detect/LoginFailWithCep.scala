package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


/**
  * TODO 从流中，根据CEP，也就是模式匹配，检测出异常登录，形成报警信息
  * 报警的业务逻辑：2秒内，两次紧邻的登录失败
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(6)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)


    //TODO 定义模式匹配的规则，具体操作的话，就是构造一个Pattern对象
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      //TODO 第一个匹配上的事件，满足eventType=fail，如果匹配上，这个事件的名字是firstFail
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail") // 第一次登录失败
      //TODO 第一个eventType=fail的事件，后面必须紧跟着eventType == "fail"的事件，如果匹配上了，这个事件叫做secondFail
      .next("secondFail").where(_.eventType == "fail") // 第二次登录失败
      //TODO 本次匹配，第一个事件和最后一个事件的时间间隔，必须在5秒内。如果超过5秒了，也会放在流里面，需要单独的提取
      .within(Time.seconds(5))

    //TODO 把模式匹配的规则，应用于流上，进行匹配，会得到一个PatternStream模式匹配流
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    //TODO 模式匹配流里面有精确匹配上的事件串，和超时匹配的事件串，把精确匹配的事件串给取出来，形成报警信息，输出出去
    val warningStream = patternStream.select(new LoginFailMatch())

    warningStream.print()
    env.execute("login fail detect with cep job")
  }
}



//TODO 自定义模式匹配流的提取方法，输入的是模式匹配流，输出的是自定义的类型。每一个匹配上的事件串，构造成一个Map，Map里面的key，是事件串里面每个事件的别名，value，是一个集合，里面存着匹配上的事件。如果只是匹配一次的话，那这个集合里就一个事件，如果用了times，比如 time（3），那这个集合里面就三个事件
class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
    //TODO 根据取出来的事件串，构造出自定义的对象，这些对象，就替代了原有的PatternStream，形成了新的流，也就是报警信息流
    //TODO 如果模式是A后面是B，但不必紧邻，B后面紧邻C，那么匹配上的事件串就是ABC，而不是A，A和B之间的DE，C
    val firstFailEvent = pattern.get("firstFail").iterator().next()
    val secondFailEvent = pattern.get("secondFail").iterator().next()
    Warning(firstFailEvent.userId, firstFailEvent.eventTime, secondFailEvent.eventTime, "login fail")
  }
}

