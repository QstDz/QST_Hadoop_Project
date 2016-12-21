package qst.streamTest

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source
import org.apache.spark.sql.SparkSession
/**
  * Created by Zz on 2016/12/17.
  */
object showSimulation {
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      System.err.println("Usage:<port><time>")
      System.exit(1)
    }
    val spark = SparkSession.builder().config("spark.sql.warehouse.dir","file:///").appName("duqu").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val data_rdd = spark.sparkContext.textFile("E:\\HadoopLocalTest\\input\\2015-11\\66\\01\\*")
    val data_arr = data_rdd.collect()
    val len = data_arr.length
    val regex="(\\d+.\\d+.\\d+.\\d+) [^ ]* [^ ]* \\[([^ ]* [^ ]*)\\] \"[^ ]+ ([^ ]+) .*\" \\d+ \\d+ \"(.*)\" \"(.*)\"".r

    val listener = new ServerSocket(args(0).toInt)
    while(true){
      val socket = listener.accept()
      new Thread(){
        override def run = {
          println("GotClient connexted from:" + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(),true)
          while(true){

            for(i <- 0 to len-1){
              Thread.sleep(args(1).toLong)
              val regex(ip,time,url,refer,agent) = data_arr(i)
              println(url)
              out.write(url+"\n")
              out.flush()
            }
          }
          socket.close()
        }
      }.start()

    }

    spark.stop()
  }
}
