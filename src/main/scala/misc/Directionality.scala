package misc

@SerialVersionUID(101L)
object Directionality extends Enumeration with Serializable  {
  type Directionality = Value
  val DIRECT,
      INVERSE = Value
}
