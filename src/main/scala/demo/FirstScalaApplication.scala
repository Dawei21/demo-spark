package demo

import javafx.application.Application
import javafx.stage.Stage
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 直接继承自Application导致的副作用:
  * 1. 无法接受命令行参数。因为args参数不会被传入
  * 2. 在Scala中，如果一个程序是多线程的，那么这个程序必须具有一个main方法。所以第二种写法只能适用于单线程的程序
  * 3. Application这个接口在执行一个程序的代码之前，需要进行一些初始化。而某些JVM不会对这些初始化代码进行优化。
  */

object FirstScalaApplication extends Application {

  val sparkConf = new SparkConf()
  //sparkConf.setAppName("Test Hello world")

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

  override def start(primaryStage: Stage): Unit = new Stage()
}
