// low performance
def toBinaryStringLimited(x : Any) : String = {
  def convertBinary(src_str : String,
                    count_size : Int
                   ) : String = {
    val res = Array.fill(count_size)('0')
    val pivot = src_str.size - count_size
    val stringBinary = src_str.splitAt(pivot)._2
    stringBinary.copyToArray(res, count_size - stringBinary.size)
    res.mkString
  }
  x match {
    case x : Byte =>
      convertBinary(x.toBinaryString,8)
    case x : Char =>
      convertBinary(x.toBinaryString,16)
    case x : Short =>
      convertBinary(x.toBinaryString,16)
    case x : Int =>
      convertBinary(x.toBinaryString,32)
    case x : Long =>
      convertBinary(x.toBinaryString,32)
    case x : Float => 
      convertBinary(x.toLong.toBinaryString,32)
    case x : Double =>
      convertBinary(x.toLong.toBinaryString,32)
  }
}