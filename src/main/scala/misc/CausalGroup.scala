package misc

@SerialVersionUID(100L)
class CausalGroup[T](val group1: Set[T], val group2: Set[T]) extends Serializable {

  def getFirstGroup(): Set[T] = {
    return group1
  }

  def getSecondGroup(): Set[T] = {
    return group2
  }

  override def toString = s"CausalGroup($group1, $group2)"
}
