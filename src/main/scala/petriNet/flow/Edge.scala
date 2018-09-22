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

  override def toString = if (direct) s"Edge($event, $state)" else s"Edge($state, $event)"
}
