package misc

@SerialVersionUID(100L)
class CausalGroup[T](val group1: Set[T], val group2: Set[T]) extends Serializable {

  def getFirstGroup(): Set[T] = {
    return group1
  }

  def getSecondGroup(): Set[T] = {
    return group2
  }

  def canEqual(a: Any) = a.isInstanceOf[CausalGroup[T]]

  override def equals(that: Any): Boolean =
    that match {
      case that: CausalGroup[T] => that.canEqual(this) &&
        this.getFirstGroup() == that.getFirstGroup() &&
        this.getSecondGroup() == that.getSecondGroup() &&
        this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + group1.hashCode()
    result = prime * result + group2.hashCode()
    return result
  }

  override def toString = s"CausalGroup($group1, $group2)"
}
