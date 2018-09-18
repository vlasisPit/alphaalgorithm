package misc

/**
  * Pair members should be strings and not Generics, because we are going to use them as keys
  * @param member1
  * @param member2
  */
@SerialVersionUID(100L)
class Pair (val member1: String, val member2: String) extends Serializable {

  def getFirstMember(): String = {
    return member1
  }

  def getSecondMember(): String = {
    return member2
  }

  def canEqual(a: Any) = a.isInstanceOf[Pair]

  override def equals(that: Any): Boolean =
    that match {
      case that: Pair => that.canEqual(this) &&
        this.getFirstMember() == that.getFirstMember() &&
        this.getSecondMember() == that.getSecondMember() &&
        this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + member1.hashCode()
    result = prime * result + member2.hashCode()
    return result
  }

  override def toString = s"$member1$member2"
}
