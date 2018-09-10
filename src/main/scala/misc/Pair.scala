package misc

@SerialVersionUID(100L)
class Pair[T] (val member1: T, val member2: T) extends Serializable {

  def getFirstMember(): T = {
    return member1
  }

  def getSecondMember(): T = {
    return member2
  }

  override def toString = s"Pair($member1, $member2)"
}
