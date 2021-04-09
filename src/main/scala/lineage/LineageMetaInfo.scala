package lineage

case class SqlOrigin(line:Int,position:Int)

case class ColumnInfo(
                     id:String,
                     name:String,
                     parents:List[String],
                     sqlOrigin: SqlOrigin
                     )
case class LineageMetaInfo(id:String,columnInfos: List[ColumnInfo])
