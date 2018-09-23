package petriNet.flow

import petriNet.state.State

/**
  * Direct == true -> (event, State)
  * Inverse == false -> (State, event)
  *
  * @param event
  * @param state
  * @param direct
  */
@SerialVersionUID(100L)
class Edge(val event: String, val state: State, val direct: Boolean)  extends Serializable {

  def getState(): State = {
    return state
  }

  def getEvent(): String = {
    return event
  }

  def isDirect(): Boolean = {
    return direct
  }

  def canEqual(a: Any) = a.isInstanceOf[Edge]

  override def equals(that: Any): Boolean =
    that match {
      case that: Edge => that.canEqual(this) &&
        this.getState() == that.getState() &&
        this.getEvent() == that.getEvent() &&
        this.isDirect() == that.isDirect() &&
        this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + event.hashCode()
    result = prime * result + state.hashCode()
    return result
  }

  override def toString = if (direct) s"Edge($event, $state)" else s"Edge($state, $event)"
}
