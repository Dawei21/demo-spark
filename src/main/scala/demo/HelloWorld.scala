package demo


import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

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

    val sc = new SparkContext(sparkConf)

    val  data = List((1, "a"), (1, "a"), (1, "b"), (2, "Key"), (3, "val"))

    val dataMap = sc.parallelize(data)
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .map(item => (item._1._1, (item._1._2, item._2)))
      .sortByKey()
      .saveAsTextFile("")


    val input_some: RDD[(Int, String)] = sc.parallelize(data)

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
	
	def getTimeBeforeByLen(cycleLen: Int): LocalDate = {
		val calendar = Calendar.getInstance()
		calendar.add(Calendar.DATE, -(1 + cycleLen))
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
		val time = dateFormat.format(calendar.getTime)
		LocalDate.parse(time, DateTimeFormatter.ofPattern("yyyyMMdd"))
	}
	
	val pathDateFormatter = new SimpleDateFormat("yyyyMMdd")
	
	
	/**
	 * 获取时间标记字符串
	 *
	 * @param calendar 结束时间
	 * @param cycle    周期
	 */
	def getFormatDateCycleStr(calendar: Calendar, cycle: Int): String = {
		
		val calendarTemp = Calendar.getInstance()
		calendarTemp.setTime(calendar.getTime)
		val endTime = calendarTemp.getTime
		val endTimeStr = pathDateFormatter.format(endTime)
		calendarTemp.add(Calendar.DATE, -cycle)
		val startTime = calendarTemp.getTime
		val startTimeStr = pathDateFormatter.format(startTime)
		startTimeStr + "-" + endTimeStr
	}
	
	
	def main(args: Array[String]): Unit = {
		
		val calendar: Calendar = Calendar.getInstance()
		val dayInWeek = calendar.get(Calendar.DAY_OF_WEEK)
		println(dayInWeek)
		if (dayInWeek == Calendar.FRIDAY) {
			return
		}
		println(getFormatDateCycleStr(calendar, 6))
		
		val FULL_SOURCE_PATH =
			"hdfs://zjyprc-hadoop/user/h_scribe/miui/miui_vip_api/year=%04d/month=%02d/day=%02d"
		val beginDate = getTimeBeforeByLen(7)
		val endDate = getTimeBeforeByLen(0)
		val filePathList: List[String] = formatThePathList(FULL_SOURCE_PATH, beginDate, endDate)
		filePathList.foreach(println)
		//		var cycleLenTemp:Int = 7
		//		var pathList: List[String] = Nil
		//		//逐条去取
		//		while (cycleLenTemp > 0) {
		//			calendar.add(Calendar.DATE, -cycleLenTemp)
		//			val yyyy = calendar.get(Calendar.YEAR)
		//			val MM = calendar.get(Calendar.MONTH)
		//			val dd = calendar.get(Calendar.DATE)
		//			//#3、过滤数据
		//			pathList = pathList ::: (FULL_SOURCE_PATH.format(yyyy, MM, dd) :: Nil)
		//			cycleLenTemp -= 1
		//		}
		//
		//		pathList.foreach(println)
		//
		//		val path1Year: List[String] = between(pathPrefix, getYestoday, getYestoday)
		//
		//		println("")
		//
		//		path1Year.foreach(println)
		//
		//		println("Start ~~")
		//
		//		val currentTime = System.currentTimeMillis()
		//		println(currentTime)
		//		val dateTime = new Date()
		//
		//		println(new SimpleDateFormat("yyyy-MM-dd").format(dateTime))
		//
		//		val calendar = Calendar.getInstance()
		//
		//		println(calendar.getFirstDayOfWeek)
		//
		//		calendar.setTimeInMillis(currentTime)
		//
		//		println(calendar.get(Calendar.YEAR))
		//		println(calendar.get(Calendar.MONTH))
		//		println(calendar.get(Calendar.DAY_OF_MONTH))
		//
		//
		//		val sparkConf = new SparkConf()
		//		sparkConf.setAppName("local")
		//
		//		val sparkContent = new SparkContext(sparkConf)
		//
		//		val data = Seq((1, "a"), (2, "Key"), (3, "val"))
		//
		//		val input_some: RDD[(Int, String)] = sparkContent.parallelize(data)
		//
		//		/*implicit : Scala在面对编译出现类型错误时，提供了一个由编译器自我修复的机制，编译器试图去寻找一个隐式implicit的转换方法，转换出正确的类型，完成编译
		//		  两个作用：
		//		  1、隐式参数(implicit parameters)
		//		  2、隐式转换(implicit conversion)
		//		 */
		//		implicit val sort_integer_by_string = new Ordering[Int] {
		//			override def compare(x: Int, y: Int): Int = String.valueOf(x).compareTo(String.valueOf(y))
		//		}
		//
		//		input_some.sortByKey()
		//
		//		println(input_some)
		//		println("ssds")
	}
	
	
	def formatThePathList(pathPrefix: String, fromDate: LocalDate, toDate: LocalDate): List[String] = {
		fromDate.toEpochDay.to(toDate.toEpochDay)
			.map(LocalDate.ofEpochDay)
			.map(time => pathPrefix.format(time.getYear, time.getMonthValue, time.getDayOfMonth))
			.toList
	}
	
	
	def getYestoday(): LocalDate = {
		val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
		val cal: Calendar = Calendar.getInstance()
		cal.add(Calendar.DATE, -1)
		val yesterday = dateFormat.format(cal.getTime())
		val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
		val date2 = LocalDate.parse(yesterday, fmt)
		date2
	}
	
	def between(path: String, fromDate: LocalDate, toDate: LocalDate): List[String] = {
		fromDate.toEpochDay.to(toDate.toEpochDay)
			.map(LocalDate.ofEpochDay)
			.map(e => path + "/year=" + e.getYear + "/month=" + "%02d".format(e.getMonthValue) + "/day=" + "%02d".format(e.getDayOfMonth))
			.toList
	}
	
	//	def tett() : Unit = {
	//		val sparkConf = new SparkConf()
	//		//sparkConf.setAppName("Test Hello world")
	//
	//		val sc = new SparkContext(sparkConf)
	//
	//
	//		val employeeRdd = sc.textFile("/home/sinbad/work/workspace/temp/employee_id_list")
	//		 employeeRdd.collect().toSet.foreach(println)
	//
	//
	//		val data = Seq((1, "a"), (2, "Key"), (3, "val"))
	//
	//		val input_some: RDD[(Int, String)] = sparkContent.parallelize(data)
	//
	//		/*implicit : Scala在面对编译出现类型错误时，提供了一个由编译器自我修复的机制，编译器试图去寻找一个隐式implicit的转换方法，转换出正确的类型，完成编译
	//    两个作用：
	//    1、隐式参数(implicit parameters)
	//    2、隐式转换(implicit conversion)
	//   */
	//		implicit val sort_integer_by_string = new Ordering[Int] {
	//			override def compare(x: Int, y: Int): Int = String.valueOf(x).compareTo(String.valueOf(y))
	//		}
	//
	//
	//		input_some.sortByKey()
	//
	//
	//		println(input_some)
	//		println("ssds")
	//	}
	//  override def start(primaryStage: Stage): Unit = new Stage()
}
