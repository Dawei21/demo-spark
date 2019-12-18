package demo

import java.io.Serializable
import java.util.Date

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken

/**
 *
 * @author Dawei on 2019/12/18 
 *
 */
class JsonDemo (val id: Long,
				val name: String,
				val birthday: Date,
				val markValue: Long) extends Serializable {

}


object JsonDemo {
	
	
	def main(args: Array[String]): Unit = {
		val str: String = "{\"id\":4,\"name\":\"demoPojo3\",\"markValue\":3}"
	
		val demo = turnJson(str)
		println(demo)
	}
	
	def turnJson(strJson: String): JsonDemo = {
		val gson: Gson = new Gson()
		val typeClass = new TypeToken[JsonDemo](){}.getType
		val value = gson.fromJson(strJson, typeClass)
		print(gson.toJson(value))
		value
	}
}


