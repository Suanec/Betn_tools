# python syntax
from tensorflow import feature_column as fc
import pydoc
fc._allowed_symbols.sort()
path = "tensorflow.feature_column."
for s in fc._allowed_symbols:
     print(s + "    " + pydoc.render_doc(path+s).split("Args:")[1].split('Returns:')[0] + "\n")
f = open("fc.all","wb")
for s in fc._allowed_symbols:
    f.write(s + "    " + pydoc.render_doc(path+s).split("Args:")[1].split('Returns:')[0] + "\n")

/// scala syntax
val path = "/Users/enzhao/weiclientupdate"
val file = path + "/fc.all"
val contents = io.Source.fromFile(file).getLines.toArray
val header = contents.zipWithIndex.filter(!_._1.startsWith(" "))
val idx = (0 until header.size-1).map( i => header(i)._2 -> header(i+1)._2) :+ (header.last._2,contents.size)
val slices = idx.map( pair => contents.slice(pair._1,pair._2)).map( arr => arr.head.trim -> arr.tail.filter(_.size > 0))
def mapBuilder(_pair : (String, Array[String])) = {
  val key = _pair._1
  val content = _pair._2
  val value = (0 until content.size).map(x => new java.lang.StringBuilder)
  var value_idx = 0
  (0 until content.size).map{
    i =>
      val contentSplits = content(i).split(':')
      val oldLine = content(i).trim.size > 0 && content(i)(6) == ' '
      // println(content(i), oldLine)
      if(!oldLine){
        value_idx += 1
        val key_value = content(i).trim.replaceFirst(":","\t")
        value(value_idx).append(key_value)
        // println(value_idx,content(i))
      } else {
        value(value_idx).append(content(i).trim)
      }
  }
  value.filter(!_.toString.isEmpty).map(v => key -> v.toString)
}
val pairs = slices.map(mapBuilder).flatten.map(kv => kv._1 + "\t" + kv._2)
pairs.foreach(println)
val fh = new java.io.FileWriter(path + "/table.fc.all")
pairs.foreach(line => fh.write(line + "\n")) 
fh.flush
