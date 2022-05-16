package sample.engine

import org.apache.spark.sql.functions.{avg, col, lit, max, min}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{BufferedWriter, FileWriter}
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

object Part2TaskRunner {

  def task3AvgBathroomsBedrooms(df: DataFrame, spark: SparkSession): DataFrame = {
    val avgDf = df.filter(df("price") > 5000 && df("review_scores_value") === 10)
      .agg(avg("bathrooms") as "avg_bathrooms", avg("bedrooms") as "avg_bedrooms")
    writeDFasCSV(avgDf, s"${File("").toAbsolute.toString}/out/out_2_3a.txt")
    writeDFasPlainCSV(avgDf, s"${File("").toAbsolute.toString}/out/out_2_3a_plain.txt")
    avgDf
  }

  def task2PrintMinMax(df: DataFrame, spark: SparkSession): DataFrame = {
    val minMaxDf = df.agg(min("price") as "min_price", max("price") as "max_price")
      .withColumn("row_count", lit(df.count()))
    writeDFasPlainCSV(minMaxDf, s"${File("").toAbsolute.toString}/out/out_2_2a_plain.txt")
    minMaxDf
  }

  def task4PeopleCountLowestPriceHighestRating(df: DataFrame, spark: SparkSession): Unit = {
    import spark.implicits._
    val lowestPriceHighestRatingRow = df.sort(col("price").asc,col("review_scores_value").desc).limit(1)
      .map(_.getAs[Double]("accommodates")).collectAsList().get(0)
    val bw = buildBufferedWriter(s"${File("").toAbsolute.toString}/out/out_2_4b.txt")
    Try(bw.write(s"Count: \n${lowestPriceHighestRatingRow}")) match {
      case Success(_) =>
      case Failure(exception) => throw new Exception(s"Failed to write distinct products count output: ${exception.getMessage}", exception)
    }
    bw.close()
  }

  private def writeDFasCSV(df: DataFrame, file: String): Unit = {
    Try(df.repartition(1).write.mode("overwrite").csv(file)) match {
      case Success(_) =>
      case Failure(exception) => throw new Exception(s"Failed to write data to. ${exception.getMessage}", exception)
    }
  }

  private def writeDFasPlainCSV(df: DataFrame, file: String): Unit = {
    val columns = df.schema.map(_.name);
    val bw = buildBufferedWriter(file)
    Try(bw.write(s"${columns.mkString(",")}")) match {
      case Success(_) =>
      case Failure(exception) => throw new Exception(s"Failed to write output schema: ${exception.getMessage}", exception)
    }
    for (elem <- df.collect()) {
      val values = columns.map(col => elem.getAs[AnyVal](col)).mkString(",")
      Try(bw.write(s"Count: \n${values}")) match {
        case Success(_) =>
        case Failure(exception) => throw new Exception(s"Failed to write output schema: ${exception.getMessage}", exception)
      }
    }
    bw.close()
  }

  def buildBufferedWriter(filePath: String) =  {
    val file = new java.io.File(filePath)
    if(!file.exists()) file.createNewFile()
    Try(new BufferedWriter(new FileWriter(file))) match {
        case Success(writer) => writer
        case Failure(exception) => throw new Exception(s"Failed to load file to write distinct products count output ${exception.getMessage}", exception)
      }
  }
}
