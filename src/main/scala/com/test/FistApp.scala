package com.test

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object FistApp {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("/Users/tandemac/workspace/lagou/TanFlink/data/wordcount.txt")

    //    val counts = text
    //      .flatMap(_.toLowerCase().split("\\W+"))
    //      .filter(_.nonEmpty)
    //      .map((_, 1))
    //      .groupBy(0)
    //      .sum(1)


    // 分词、转换、求和
    val ws = text.flatMap(_.split(" ")).map((_, 1)).groupBy(0).sum(1)
    ws.print()


    //    env.execute("wordcount")


  }
}
