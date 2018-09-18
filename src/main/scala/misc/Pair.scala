package misc

@SerialVersionUID(100L)
class Pair[T] (val member1: T, val member2: T) extends Serializable {

  def getFirstMember(): T = {
    return member1
  }

  def getSecondMember(): T = {
    return member2
  }

  def canEqual(a: Any) = a.isInstanceOf[T]
  override def equals(that: Any): Boolean =
    that match {
      case that: String => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + (if (member1 == null) 0 else member1.hashCode)
    result = prime * result + (if (member2 == null) 0 else member2.hashCode)
    return result
  }

  override def toString = s"Pair($member1, $member2)"
}
