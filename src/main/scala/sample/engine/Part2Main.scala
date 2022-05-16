package sample.engine

import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

object Part2Main {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder().master("local[*]").appName("Solution").getOrCreate()
    val airBnbDf = Try(getClass.getClassLoader.getResource("sf-airbnb-clean.parquet").getPath) match {
      case Success(path) => Try(spark.read.parquet(path)) match {
        case Success(parquetDF) => parquetDF
        case Failure(csvDFException) => throw new Exception(s"Failed to read sf-airbnb data from $path. ${csvDFException.getMessage}", csvDFException)
      }
      case Failure(exception) => throw new Exception(s"Failed to load sf-airbnb file. ${exception.getMessage}", exception)
    }
    println("Printing first 5 rows")
    airBnbDf.show(5)
    val minMaxDf = Part2TaskRunner.task2PrintMinMax(airBnbDf, spark)
    minMaxDf.show()
    val avgBedBathDf = Part2TaskRunner.task3AvgBathroomsBedrooms(airBnbDf, spark)
    avgBedBathDf.show(5)
    Part2TaskRunner.task4PeopleCountLowestPriceHighestRating(airBnbDf, spark)

  }

}
