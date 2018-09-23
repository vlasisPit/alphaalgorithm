package petriNet.state

/**
  * Class places depicts a 3-tuple consisting of the initial state, the final state
  * and all the other possible states in the petri net.
  * @param places
  */
@SerialVersionUID(100L)
class Places(val places: (State, State, List[State])) extends Serializable {

  def getStates(): List[State] = {
    return places._3
  }

  def getInitialState(): State = {
    return places._1
  }

  def getFinalState(): State = {
    return places._2
  }
}
