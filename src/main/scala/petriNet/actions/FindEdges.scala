package petriNet.actions

import petriNet.flow.Edge
import petriNet.state.{Places, State}

@SerialVersionUID(100L)
class FindEdges(val places: Places) extends Serializable {

  def find():List[Edge] = {
    val edges = places.getStates()
      .flatMap(x=>constructEdgesFromStates(x))

    return edges ::: getInitialEdges() ::: getFinalEdges()
  }

  def constructEdgesFromStates(state : State) : List[Edge] = {
    val inputEdges = state.getInput()
      .map(x=> new Edge(x, state, true))
      .toList

    val outputEdges = state.getOutput()
      .map(x=> new Edge(x, state, false))
      .toList

    return inputEdges ::: outputEdges
  }

  def getInitialEdges(): List[Edge] = {
    return places.getInitialState().getOutput()
      .map(x=> new Edge(x, places.getInitialState(), false))
      .toList
  }

  def getFinalEdges(): List[Edge] = {
    return places.getFinalState().getInput()
      .map(x=> new Edge(x, places.getFinalState(), true))
      .toList
  }

}
