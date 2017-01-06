import java.util
import java.util.{ArrayList, Date}

import com.mongodb.{ServerAddress, MongoClient, MongoCredential}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.bson.Document
import org.bson.types.ObjectId

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MCYarn on 2017/1/6.
  */
class AddToMongo {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("load data")
    //val conf = new SparkConf( ).setAppName( "initialization" )
    //序列化
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    //心跳超时
    conf.set("spark.worker.timeout","6000")
    //针对shuffle进行优化
    val ser = conf.get("spark.serializer")
    //设置每个stage的task数量
    //    conf.set("spark.default.parallelism","216")
    conf.set("spark.storage.memoryFraction","0.5")

    val sTime = System.currentTimeMillis()

    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("hdfs://master:9000/test_H/testFile1.nb").filter{line =>
      val pair = line.split("\\s+")
      if("".equals(pair(1)) || "null".equals(pair(1)) || "".equals(pair(2)) || "".equals(pair(3))
         || "".equals(pair(4)) || "".equals(pair(6)) || "".equals(pair(7)) || "".equals(pair(8))
         || "".equals(pair(9)) || "".equals(pair(10)) || "".equals(pair(11))){
        false
      }else{
        true
      }
    }

    val relation1  = rdd.map(line => {
      val pair = line.split("\\s+")
      (((pair(6).toLong,pair(1),pair(2)),( pair(7).toLong,pair(3),pair(4))),((pair(8),1,pair(10).toLong,pair(11).toLong)))
    }) ++ rdd.map(line => {
      val pair = line.split("\t")
      (((pair(7).toLong,pair(3),pair(4)),(pair(6).toLong,pair(1),pair(2))), (pair(8),-1,pair(10).toLong,pair(11).toLong))
    }).distinct()

    val res = relation1.aggregateByKey(ArrayBuffer[(String,Int,Long,Long)]())( _:+_,_++_ ).
      map( x=>(x._1._1,(x._1._2,new ObjectId( new Date( ) ),x._2 ))).
      aggregateByKey(ArrayBuffer[((Long,String,String),ObjectId,ArrayBuffer[(String,Int,Long,Long)])]())( _:+_,_++_ )


    import scala.collection.JavaConversions._
    val info = res.mapPartitionsWithIndex( ( index,ee ) =>{
      val res = scala.collection.mutable.ArrayBuffer.empty[(Int, ArrayList[Document],ArrayList[Document],ArrayList[Document])]
      val relation = new ArrayList[Document]( )
      val property = new ArrayList[Document]( )
      val side = new ArrayList[Document]()

      ee.foreach( e =>{
        val obj = new Document( "id",e._1._1 ).append( "val",e._1._3 ).append("ft",e._1._2 )
        property.add(obj)

        val list = new ArrayList[Document]( )
        e._2.foreach( part =>{
          val dst_temp = new Document("dst_id",part._1._1).append( "key",part._2)

          val subList = new ArrayList[Document]()
          part._3.foreach{ z=>
            val s = new Document("dr",z._2).append("type",z._1).append("start",z._3).append("end",z._4)
            subList.add(s)
          }
          side.add(new Document("key",part._2).append("Item",subList))
          list.add(dst_temp)
        })
        val temp = new Document("id",e._1._1).append("friend",list)
        relation.add(temp)
      })

      res.+=((index,relation,property,side))
      res.iterator
    }).repartition(40).cache()
/*
    val source_temp1 = rdd.map(line =>{
      val pair = line.split("\\s+")
      ((pair(6),pair(7)),(pair(6),pair(2),pair(1)),(pair(7),pair(4),pair(3)),(pair(8),1,pair(9),pair(10),pair(11)))
    })
    val source_temp2 = rdd.map(line =>{
      val pair = line.split("\\s+")
      ((pair(7),pair(6)),(pair(7),pair(4),pair(3)),(pair(6),pair(2),pair(1)),(pair(8),-1,pair(9),pair(10),pair(11)))
    })
    val source = (source_temp1 ++ source_temp2).distinct()


    val res =source.map( line =>{
      (line._1._1,(line._1._2,new ObjectId( new Date() ),line._4 ))
    }).aggregateByKey(ArrayBuffer[((String,ObjectId),ArrayBuffer[(String,Int,String,String,String)])]())( _:+_,_++_ )
*/
val mTable = "hyh_test"
    info.foreachPartition( x =>{
      val array = new ArrayList[String]()
      array.add("192.168.0.22:40000")
      array.add("192.168.0.23:40000")
      array.add("192.168.0.24:40000")
      x.foreach { part =>
        try {
          val credential1:MongoCredential = MongoCredential.createCredential( "asideal"
            ,"admin","1q2w3e4r".toCharArray( ) );
          val credentials1:ArrayList[MongoCredential] = new ArrayList[MongoCredential]()
          credentials1.add( credential1 )
          val mongo:MongoClient = new MongoClient( new ServerAddress( array.get(part._1%array.length ) ) ,credentials1 )
          val relation = mongo.getDatabase(mTable).getCollection("relation")
          val property = mongo.getDatabase(mTable).getCollection("property")
          val side = mongo.getDatabase(mTable).getCollection("side")
          relation.insertMany( part._2 )
          property.insertMany( part._3 )
          property.insertMany( part._4 )
        } catch {
          case e: Exception => println(e.getMessage)
        }
      }
    })
    val eTime = System.currentTimeMillis()

    println("spend times:"+( eTime-sTime ))

  }

}