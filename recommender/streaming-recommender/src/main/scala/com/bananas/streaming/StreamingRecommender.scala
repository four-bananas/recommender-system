package com.bananas.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

// 定义连接助手对象，序列化
object ConnHelper extends Serializable{
  // 会讲用户最近评分的数据存储到redis
  val jedis = new Jedis("cdh1" , 6379)
  lazy val mongoClient = MongoClient( MongoClientURI("mongodb://cdh1:27017/recommender") )
}

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation( mid: Int, score: Double )

// 定义基于预测评分的用户推荐列表
case class UserRecs( uid: Int, recs: Seq[Recommendation] )

// 定义基于LFM电影特征向量的电影相似度列表
case class MovieRecs( mid: Int, recs: Seq[Recommendation] )



object StreamingRecommender {

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_MOVIE_RECS_COLLECTION = "MovieRecs"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://cdh1:27017/recommender",
      "mongo.db" -> "recommender",
      "kafka.topic" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StreamingRecommender")
    sparkConf.registerKryoClasses(Array(classOf[MongoConfig], classOf[Recommendation], classOf[UserRecs], classOf[MovieRecs]))

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    // 拿到streaming context
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(2)) // batch duration

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加载电影相似度矩阵数据，把它广播出去 , 一个excutor上留存一个副本, 不会在每个任务上都保存副本, 比较节省内存资源
    val simMovieMatrix = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{ movieRecs => // 为了查询相似度方便，转换成map, 一个mid, 一个列表, 但是查询速度不够快, 将每一个元素转换成为map就能够查询更快一些, 整体转换成map
        (movieRecs.mid, movieRecs.recs.map( x=> (x.mid, x.score) ).toMap )
      }.collectAsMap() // 整体转为map
     // 加载电影相似度矩阵数据，把它广播出去 , 一个excutor上留存一个副本, 不会在每个任务上都保存副本, 比较节省内存资源

    // 定义广播变量, 将数据广播出去
    val simMovieMatrixBroadCast = sc.broadcast(simMovieMatrix)

    // 定义kafka连接参数
    val kafkaParam = Map(
      "bootstrap.servers" -> "cdh1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recommender",
      "auto.offset.reset" -> "latest"
    )

    // 通过kafka创建一个DStream
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,   // 偏向连续的策略
      // 根据kafkaParam创建一个消费者, 订阅了我们的topic, 如果kafka那边来数据, 我们这边就可以获取到了, 就把数据打通了
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaParam) // param1: topic列表, kafka的配置项
    )

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map{
      msg =>
        val attr = msg.value().split("\\|")
        ( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    }

    // 继续做流式处理，核心实时算法部分
    ratingStream.foreachRDD{
          //rdds, 是某一个时间窗口中的一组rdd
      rdds => rdds.foreach{ // feach 遍历, 获取rdd的具体类容
        case (uid, mid, score, timestamp) => {
          println("rating data coming! >>>>>>>>>>>>>>>>")

          // 1. 从redis里获取当前用户最近的K次评分，保存成Array[(mid, score)]
          val userRecentlyRatings = getUserRecentlyRating( MAX_USER_RATINGS_NUM, uid, ConnHelper.jedis )

          // 2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表，Array[mid]
          val candidateMovies = getTopSimMovies( MAX_SIM_MOVIES_NUM, mid, uid, simMovieMatrixBroadCast.value )

          // 3. 将备选电影列表和最近评分过的电影的评分做加权, 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
          val streamRecs = computeMovieScores( candidateMovies, userRecentlyRatings, simMovieMatrixBroadCast.value )

          // 4. 把推荐数据保存到mongodb
          saveDataToMongoDB( uid, streamRecs )
          println("<<<<<<<<<<<<<<<<<  rating data coming! >>>>>>>>>>>>>>>>")
        }
      }
    }

    // 开始接收和处理数据
    ssc.start()
    println("+++++++++++++++++ streaming started! +++++++++++++++++++")
    ssc.awaitTermination()   // 一直等待处理, 除非手动停止或者异常退出


  }

  // redis操作返回的是java类，为了用map操作需要引入转换类
  import scala.collection.JavaConversions._

  def getUserRecentlyRating(num: Int, uid: Int, jedis: Jedis): Array[(Int, Double)] = {
    val jedis1 = new Jedis("cdh1" , 6379)
    jedis1.select(2)
    // 从redis读取数据，用户评分数据保存在 uid:UID 为key的队列里，value是 MID:SCORE
    jedis1.lrange("uid:" + uid, 0, num-1)
      .map{
        item => // 具体每个评分又是以冒号分隔的两个值
          val attr = item.split("\\:")
          //返回电影的评分
          ( attr(0).trim.toInt, attr(1).trim.toDouble )
      }
      .toArray
  }

  /**
   * 获取跟当前电影做相似的num个电影，作为备选电影
   * @param num       相似电影的数量
   * @param mid       当前电影ID
   * @param uid       当前评分用户ID
   * @param simMovies 相似度矩阵
   * @return          过滤之后的备选电影列表
   */
  def getTopSimMovies(num: Int, mid: Int, uid: Int, simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                     (implicit mongoConfig: MongoConfig): Array[Int] ={
    // 1. 从相似度矩阵中拿到所有相似的电影
    val allSimMovies = simMovies(mid).toArray

    // 2. 从mongodb中查询用户已看过的电影
    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
      .find( MongoDBObject("uid" -> uid) )
      .toArray
      .map{
        item => item.get("mid").toString.toInt
      }

    // 3. 把看过的过滤，得到输出列表
    allSimMovies.filter( x=> ! ratingExist.contains(x._1) )
      .sortWith(_._2>_._2)
      .take(num)
      .map(x=>x._1)
  }


  // 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表
  def computeMovieScores(candidateMovies: Array[Int],
                         userRecentlyRatings: Array[(Int, Double)],
                         simMovies: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]]): Array[(Int, Double)] ={
    // 定义一个ArrayBuffer，用于保存每一个备选电影的基础得分
    val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
    // 定义一个HashMap，保存每一个备选电影的增强减弱因子
    val increMap = scala.collection.mutable.HashMap[Int, Int]()  // 奖励项
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()  //惩罚项

    for( candidateMovie <- candidateMovies; userRecentlyRating <- userRecentlyRatings){
      // 拿到备选电影和最近评分电影的相似度
      val simScore = getMoviesSimScore( candidateMovie, userRecentlyRating._1, simMovies )
      println(s"当前评分:${simScore}")
      if(simScore > 0.7){
        // 计算备选电影的基础推荐得分

        scores += ( (candidateMovie, simScore * userRecentlyRating._2) )
        if( userRecentlyRating._2 > 3 ){
          //若果大于3, 就应该以备选电影的mid作为key 去存一个值
          increMap(candidateMovie) = increMap.getOrDefault(candidateMovie, 0) + 1
        } else{
          decreMap(candidateMovie) = decreMap.getOrDefault(candidateMovie, 0) + 1
        }
      }
    }
    // 根据备选电影的mid做groupby，根据公式去求最后的推荐评分
    scores.groupBy(_._1).map{
      // groupBy之后得到的数据 Map( mid -> ArrayBuffer[(mid, score)] )
      case (mid, scoreList) =>
        ( mid, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(mid, 1)) - log(decreMap.getOrDefault(mid, 1)) )
    }.toArray.sortWith(_._2>_._2)
  }

  // 获取两个电影之间的相似度
  def getMoviesSimScore(mid1: Int, mid2: Int, simMovies: scala.collection.Map[Int,
    scala.collection.immutable.Map[Int, Double]]): Double ={

    simMovies.get(mid1) match {
      case Some(sims) => sims.get(mid2) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  // 求一个数的对数，利用换底公式，底数默认为10
  def log(m: Int): Double ={
    val N = 10
    math.log(m)/ math.log(N)
  }

  def saveDataToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit ={
    // 定义到StreamRecs表的连接
    val streamRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    // 如果表中已有uid对应的数据，则删除
    streamRecsCollection.findAndRemove( MongoDBObject("uid" -> uid) )
    // 将streamRecs数据存入表中
    streamRecsCollection.insert(
      MongoDBObject( "uid"->uid, "recs"-> streamRecs.map(x=>MongoDBObject( "mid"->x._1, "score"->x._2 ))
      )
    )
  }

}
