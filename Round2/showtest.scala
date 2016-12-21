package qst.streamTest

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext, Time}
import qst.streamTest.StreamingText.People

/**
  * Created by Zz on 2016/12/18.
  */
object showtest {
  case class Acc(url:String)
  var count = 0
  val regex="(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"".r
  def main(args: Array[String]): Unit = {
    if(args.length < 2){
      System.err.println("Usage:NextWorkAVG <hostname><port>")
      System.exit(1)
    }
    val sparkconf = new SparkConf().setAppName("NetStreamingTest")
    val ssc = new StreamingContext(sparkconf,Seconds(1))
    val url_rdd = ssc.socketTextStream(args(0),args(1).toInt,StorageLevel.MEMORY_AND_DISK_SER)
    url_rdd.foreachRDD{(rdd:RDD[String],time:Time)=>
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      if(rdd.collect().length>0){
        val url_df=rdd.map(p=>Acc(p.toString)).toDF()
        url_df.createOrReplaceTempView("Acc")
        spark.udf.register("pd",(s:String)=>s.contains("show"))
        val sum = spark.sql("select a.show from(select pd(url) as show from Acc) a where a.show=true")
        println(s"===========$time===========")
        sum.show()
        count += sum.count().toInt
        println(count)
      }
    }
    ssc.start()
    ssc.awaitTermination()

  }
}
