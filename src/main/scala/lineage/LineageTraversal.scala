package lineage

import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan, SubqueryAlias, UnaryNode, Union}


object LineageTraversal{

  def apply(plan: LogicalPlan): LineageTraversal = new LineageTraversal(plan)

}

class LineageTraversal(plan:LogicalPlan) {

  var lineageMap  = Map[String,LineageMetaInfo]

  def traverse(plan: LogicalPlan,parent:LogicalPlan): Unit ={
    plan match {

      case node@SubqueryAlias(a,child)=>{
        traverse(child,node)
      }

      case node: UnaryNode => {
        traverse(node.child,node)
      }
      case node: BinaryNode => {
        traverse(node.left,node)
        traverse(node.right,node)
      }
      case node@Union(children) =>{
        children.map(child=>traverse(node,child))
      }
      case node: LeafNode =>{

      }




    }


  }


}
