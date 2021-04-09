package scalabasic

object patternMatching {

  trait Expr ;
  case class Number(n:Int) extends Expr
  case class Sum(e1:Expr,e2:Expr) extends Expr
  case class Product(e1:Expr,e2:Expr) extends Expr

  def decomposeExpr(e:Expr):String={

    val value = e match {
      case l@Number(x)=> l.n.toString
      case i@Sum(a,b) => s"""(${decomposeExpr(i.e1)}+${decomposeExpr(i.e2)})"""
      case k@Product(a,b)=>s"""${decomposeExpr(k.e1)}*${decomposeExpr(k.e2)}"""
      case _  => "hey"
    }

    value
  }

  def matchCmp():Unit={
    val a = List(1,2,3)
    val b = a.filter(_<=2).map(println(_))

    val s  = a match {
      case List(a:Int,_*)=> s"started with ${a.toString}"
    }
    println(s)
  }

  def matchTuple():Unit={
    val a = (2,4)

    val s = a match {
      case (a:Int,b:Int)=> s"first value is ${a.toString} and 2nd is ${b.toString}"
    }
    println(s)
  }


  def main(args: Array[String]): Unit = {
    val sum = Sum(Number(2),Number(3))

    val product1 = Product(Number(2),Number(3))
    val product = Product(product1,Number(6))
    val sum1 = Sum(product1,Number(6))
    println(decomposeExpr(product))
    println(decomposeExpr(sum1))
    matchCmp()
    matchTuple()

  }

}
