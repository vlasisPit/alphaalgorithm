package misc

@SerialVersionUID(102L)
object Relation extends Enumeration with Serializable {
  type Relation = Value
  val FOLLOW,
      NOT_FOLLOW,
      CAUSALITY,
      PARALLELISM,
      NEVER_FOLLOW = Value
}
