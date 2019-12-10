package demo


import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  *
  */
object HelloWorld {

  def main(args: Array[String]): Unit = {

    println("Start ~~")

    val currentTime = System.currentTimeMillis()
    println(currentTime)
    val dateTime = new Date()

    println(new SimpleDateFormat("yyyy-MM-dd").format(dateTime))

    val calendar = Calendar.getInstance()

    println(calendar.getFirstDayOfWeek)

    calendar.setTimeInMillis(currentTime)

    println(calendar.get(Calendar.YEAR))
    println(calendar.get(Calendar.MONTH))
    println(calendar.get(Calendar.DAY_OF_MONTH))


    val sparkConf = new SparkConf()
    sparkConf.setAppName("local")

    val sparkContent = new SparkContext(sparkConf)

    val  data = Seq((1, "a"),(2, "Key"),(3, "val"))

    val input_some: RDD[(Int, String)] = sparkContent.parallelize(data)

    /*implicit : Scala在面对编译出现类型错误时，提供了一个由编译器自我修复的机制，编译器试图去寻找一个隐式implicit的转换方法，转换出正确的类型，完成编译
      两个作用：
      1、隐式参数(implicit parameters)
      2、隐式转换(implicit conversion)
     */
    implicit val sort_integer_by_string = new Ordering[Int] {
      override def compare(x: Int, y: Int): Int = String.valueOf(x).compareTo(String.valueOf(y))
    }

    input_some.sortByKey()

    println(input_some)
    println("ssds")
  }
}
