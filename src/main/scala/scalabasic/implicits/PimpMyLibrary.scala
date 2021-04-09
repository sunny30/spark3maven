package scalabasic.implicits

object PimpMyLibrary extends App{

  case class Wrapper(value:Int)
  implicit class RichCase(wrapper:Wrapper){
    def  square:Int=wrapper.value*wrapper.value
  }

  val wrap = Wrapper(42)
  println(wrap.square)
}
/*
====batch 1==== {   "tagname": "abc123",   "tag_info": {     "value": 123,     "quality": "good"  } }  DF : tagname, tag_info_value, tag_info_quality  ===batch 2===== {   "tagname": "abc123",   "tag_info": {     "value": 124,     "quality": "good", 	"extras":{ 		"last_updated": "2020-10-12" 	}   } } 
 */