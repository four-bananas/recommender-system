package com.bananas.statistics

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}


case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri:String, db:String)

// 定义一个基准推荐对象
case class Recommendation(mid: Int, score: Double)

// 定义电影类别top10推荐对象
case class GenresRecommendation(genres: String, recs: Seq[Recommendation])


object StatisticsRecommender {

  // 定义表名
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://cdh1:27017/recommender",
      "mongo.db" -> "recommender"
    )

    // 创建一个sparkConf
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommeder")

    // 创建一个SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))


    // 从mongodb加载数据
    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    //基于sql完成数据的基本统计计算
    //    创建临时表
    ratingDF.createOrReplaceTempView("ratings")


    //计算不同的统计推荐的结果
    //1. 历史热门统计, 历史评分数据最多 mid分组, count(mid)
    val rateMoreMoviesDF = spark.sql("select mid, count(mid) as count from ratings group by mid")
    //1.1 将计算的结果存入到mongo中
    storeDFInMongoDB(rateMoreMoviesDF,RATE_MORE_MOVIES)

    //2. 最近热门电影统计
    //2.1 创建一个日期格式化的工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    //2.2 注册udf, 把时间戳转换成为年月日
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    val ratingOfYearMonth = spark.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMoviesDF = spark.sql("select mid, count(mid) as count, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, count desc")
    storeDFInMongoDB(rateMoreRecentlyMoviesDF,RATE_MORE_RECENTLY_MOVIES)
    //3. 优质电影电影平均得分统计
    val averageMoviesDF = spark.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDFInMongoDB(averageMoviesDF, AVERAGE_MOVIES)
    //4. 分别计算每种类型的电影集合中评分最高的 10 个电影
    //4.1 需要知道电影的类别一共有多少种
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery"
      , "Romance", "Science", "Tv", "Thriller", "War", "Western")

    //根据平均评分排各个类别下的top10  内连接
    val movieWithScore = movieDF.join(averageMoviesDF,"mid")

    //笛卡尔积查询, 在rdd里面
    val genresRDD = spark.sparkContext.makeRDD(genres)
    val genresTopMoviesDF = genresRDD.cartesian(movieWithScore.rdd)
      .filter{
        //条件过滤: 找出movie的字段genres值的(Action|Adventure|Sci-Fi) 包含当前类别genres (Action)那些, 把不包含的过滤掉
        case (genre, movieRow) => movieRow.getAs[String]("genres").toLowerCase.contains(genre.toLowerCase)
      }
      .map{
        //对数据做预处理 为后续处理成为Recommendation类型的数据做准备
        case (genre, movieRow) => (genre, (movieRow.getAs[Int]("mid"),movieRow.getAs[Double]("avg")))
      }
      .groupByKey()
      .map{
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map(item => Recommendation(item._1, item._2)))
      }
      .toDF()

    storeDFInMongoDB(genresTopMoviesDF, GENRES_TOP_MOVIES)

    spark.stop()

  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit  mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }



}
