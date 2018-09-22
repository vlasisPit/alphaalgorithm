package petriNet.state

@SerialVersionUID(100L)
class FinalState(finalEvents: Set[String]) extends Serializable {

  def getFinalEvents(): Set[String] = {
    return finalEvents
  }

  override def toString = s"FinalState($getFinalEvents)"
}
