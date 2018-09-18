// object genReviewListDate {
//   import java.text.SimpleDateFormat
//   import java.util.Date 
//   import java.util.Calendar
//   import java.util.TimeZone
//   def genReviewListDate(_date : Date, 
//     _cal : Calendar = Calendar.getInstance, 
//     _pattern : String = "yyyyMMdd",
//     _sdf : SimpleDateFormat = new SimpleDateFormat()
//   ):String = {
//     _sdf.setTimeZone(TimeZone.getTimeZone("GMT"+8))
//     _sdf.applyPattern(_pattern)
//     _cal.setTime(_date)
//     _cal.add(Calendar.DAY_OF_MONTH, -1)
//     val dateArr = (0 until 7).map{
//       i =>
//         _cal.add(Calendar.DAY_OF_MONTH, 1)
//         _sdf.format(_cal.getTime)
//     }.filter{
//       d =>
//         _cal.setTime(_sdf.parse(d))
//         val weekOfDay = _cal.get(Calendar.DAY_OF_WEEK)
//         (weekOfDay > 1 && weekOfDay < 7)
//     }
//     val strArr = dateArr.map{
//       s =>
//         s"#### ${s}\n> - [ ] \n> - [ ] \n> - [ ] \n"
//     }
//     s"${dateArr.head}-${dateArr.last}\n\n" + strArr.mkString("\n")
//   }
//   def genReviewListDateByString(_date : String, 
//     _cal : Calendar = Calendar.getInstance, 
//     _pattern : String = "yyyyMMdd",
//     _sdf : SimpleDateFormat = new SimpleDateFormat()
//   ):String = {
//     _sdf.applyPattern(_pattern)
//     _cal.setTime(_sdf.parse(_date))
//     _cal.add(Calendar.DAY_OF_MONTH, 1)
//     genReviewListDate(_cal.getTime,
//       _cal,_pattern,_sdf)
//   }
//   def main(args:Array[String]) : Unit = {
//     if(args.isEmpty){
//       val dateNow = new Date
//       println(genReviewListDate(dateNow))
//     }else{
//       if(args.size == 1){
//         val dateSpecified = args.head
//         if(dateSpecified.size == 8){
//           println(genReviewListDateByString(dateSpecified))
//         }else{
//           println("Date Format ERROR, please follow yyyyMMdd.")
//           System.exit(1)
//         }
//       }else{
//         println("Only need ONE paramter!! EXIT")
//         System.exit(1)
//       }
//     }
//   }
// }
object genReviewListDate extends App {
  import java.text.SimpleDateFormat
  import java.util.Date 
  import java.util.Calendar
  import java.util.TimeZone
  def genReviewListDate(_date : Date, 
    _cal : Calendar = Calendar.getInstance, 
    _pattern : String = "yyyyMMdd",
    _sdf : SimpleDateFormat = new SimpleDateFormat()
  ):String = {
    _sdf.setTimeZone(TimeZone.getTimeZone("GMT"+8))
    _sdf.applyPattern(_pattern)
    _cal.setTime(_date)
    _cal.add(Calendar.DAY_OF_MONTH, -1)
    val dateArr = (0 until 7).map{
      i =>
        _cal.add(Calendar.DAY_OF_MONTH, 1)
        _sdf.format(_cal.getTime)
    }.filter{
      d =>
        _cal.setTime(_sdf.parse(d))
        val weekOfDay = _cal.get(Calendar.DAY_OF_WEEK)
        (weekOfDay > 1 && weekOfDay < 7)
    }
    val strArr = dateArr.map{
      s =>
        // s"#### ${s}\n- [ ] \n- [ ] \n- [ ] \n"
        s"#### ${s}\n-  \n-  \n-  \n"
    }
    s"${dateArr.head}-${dateArr.last}\n\n" + strArr.mkString("\n")
  }
  def genReviewListDateByString(_date : String, 
    _cal : Calendar = Calendar.getInstance, 
    _pattern : String = "yyyyMMdd",
    _sdf : SimpleDateFormat = new SimpleDateFormat()
  ):String = {
    _sdf.applyPattern(_pattern)
    _cal.setTime(_sdf.parse(_date))
    _cal.add(Calendar.DAY_OF_MONTH, 1)
    genReviewListDate(_cal.getTime,
      _cal,_pattern,_sdf)
  }
  if(args.isEmpty){
    val dateNow = new Date
    println(genReviewListDate(dateNow))
  }else{
    if(args.size == 1){
      val dateSpecified = args.head
      if(dateSpecified.size == 8){
        println(genReviewListDateByString(dateSpecified))
      }else{
        println("Date Format ERROR, please follow yyyyMMdd.")
        System.exit(1)
      }
    }else{
      println("Only need ONE paramter!! EXIT")
      System.exit(1)
    }
  }
}

genReviewListDate.main(args)
