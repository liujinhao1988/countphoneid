

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._



object PhoneCount {


  def main(args: Array[String]) :Unit={
    val conf = new SparkConf().setMaster("local[2]").setAppName("PhoneCount")
    val ssc = new StreamingContext(conf, Seconds(3))


    val kafkaParams:Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "access-log",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" ->"org.apache.kafka.common.serialization.StringDeserializer",


    )



    val kafkaDStream:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("access-log"), kafkaParams)
    )


    val valueDStream:DStream[String]=kafkaDStream.map(record =>record.value())

    val result1 =valueDStream.flatMap(_.split(" "))
        .filter(_.nonEmpty)
        .map((_,1))



    val resultDS:DStream[(String,Int)]=result1.window(Seconds(30),Seconds(30))

    val result3=resultDS.reduceByKey(_+_)



    result3.foreachRDD { rdd =>

      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF()

      val aaa=wordsDataFrame.orderBy(-wordsDataFrame("_2"))

      val bbb=aaa.limit(5)
      bbb.show()
    }






    ssc.start()             // Start the computation
    ssc.awaitTermination()
  }

}
