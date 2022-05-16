package sample.engine

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

object Part1TaskRunner {

  def task2FindDistinctProducts(dataSet: Dataset[String], spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    val distinctProducts = dataSet.flatMap(_.split(",")).map(_.trim).distinct()
    task1PrintFirst5(distinctProducts)
    val bw = Part2TaskRunner.buildBufferedWriter(s"${File("").toAbsolute.toString}/out/out_1_2a.txt")
    Try(for(product <- distinctProducts.collect()) bw.write(s"product $product\n")) match {
      case Success(_) =>
      case Failure(exception) => throw new Exception(s"Failed to write distinct products output: ${exception.getMessage}", exception)
    }
    bw.close()
    distinctProducts
  }

  def task1PrintFirst5(dataSet: Dataset[String]): Unit = {
    dataSet.take(5).foreach(println(_))
  }

  def task3CountDistinctProducts(distinctProducts: Dataset[String]): Unit = {
    val bw = Part2TaskRunner.buildBufferedWriter(s"${File("").toAbsolute.toString}/out/out_1_2b.txt")
    Try(bw.write(s"Count: \n${distinctProducts.count()}")) match {
      case Success(_) =>
      case Failure(exception) => throw new Exception(s"Failed to write distinct products count output: ${exception.getMessage}", exception)
    }
    bw.close()
  }


}
