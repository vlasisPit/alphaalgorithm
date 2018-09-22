package petriNet.actions

import petriNet.flow.Edge
import petriNet.state.{FinalState, InitialState, State}

@SerialVersionUID(100L)
class FindEdges(val places: (InitialState, FinalState, List[State])) extends Serializable {

  def find():List[Edge] = {
    val edges = places._3
      .flatMap(x=>constructEdges(x))

    return edges
  }

  def constructEdges(state : State) : List[Edge] = {
    val inputEdges = state.getInput()
      .map(x=> new Edge(x, state, true))
      .toList

    val outputEdges = state.getOutput()
      .map(x=> new Edge(x, state, false))
      .toList

    return inputEdges ::: outputEdges
  }

}
