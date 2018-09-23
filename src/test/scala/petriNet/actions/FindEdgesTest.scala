package petriNet.actions

import org.scalatest.FunSuite
import petriNet.flow.Edge
import petriNet.state.{Places, State}

class FindEdgesTest extends FunSuite {

  test("Check FindCausalGroups correct functionality - Log 2") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("D"), Set.empty)
    val states = List(new State(Set("A"), Set("B", "C")),
                      new State(Set("B", "C"), Set("D"))
    )

    val places = new Places(initialState, finalState, states)

    val findEdges: FindEdges = new FindEdges(places)
    val edges = findEdges.find()

    //check edges
    assert(edges.contains(new Edge("A", new State(Set("A"), Set("B", "C")), true)))
    assert(edges.contains(new Edge("B", new State(Set("A"), Set("B", "C")), false)))
    assert(edges.contains(new Edge("C", new State(Set("A"), Set("B", "C")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), true)))
    assert(edges.contains(new Edge("C", new State(Set("B", "C"), Set("D")), true)))
    assert(edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), false)))

    //check initial edges
    assert(edges.contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(edges.contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(edges.size==8)
  }

}
