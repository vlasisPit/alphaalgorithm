package petriNet.state

@SerialVersionUID(100L)
class InitialState(initialEvents: Set[String]) extends Serializable {

  def getInitialEvents(): Set[String] = {
    return initialEvents
  }

  override def toString = s"Initial($getInitialEvents)"
}
