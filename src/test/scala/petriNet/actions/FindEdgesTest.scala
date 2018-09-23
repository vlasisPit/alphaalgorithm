package petriNet.actions

import org.scalatest.FunSuite
import petriNet.flow.Edge
import petriNet.state.{Places, State}

class FindEdgesTest extends FunSuite {

  test("Check FindCausalGroups correct functionality - Log 1") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("D"), Set.empty)
    val states = List(
      new State(Set("A"), Set("B", "E")),
      new State(Set("A"), Set("C", "E")),
      new State(Set("B", "E"), Set("D")),
      new State(Set("C", "E"), Set("D"))
    )

    val places = new Places(initialState, finalState, states)

    val findEdges: FindEdges = new FindEdges(places)
    val edges = findEdges.find()

    //check edges
    assert(edges.contains(new Edge("A", new State(Set("A"), Set("B", "E")), true)))
    assert(edges.contains(new Edge("B", new State(Set("A"), Set("B", "E")), false)))
    assert(edges.contains(new Edge("E", new State(Set("A"), Set("B", "E")), false)))
    assert(edges.contains(new Edge("A", new State(Set("A"), Set("C", "E")), true)))
    assert(edges.contains(new Edge("C", new State(Set("A"), Set("C", "E")), false)))
    assert(edges.contains(new Edge("E", new State(Set("A"), Set("C", "E")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B", "E"), Set("D")), true)))
    assert(edges.contains(new Edge("E", new State(Set("B", "E"), Set("D")), true)))
    assert(edges.contains(new Edge("D", new State(Set("B", "E"), Set("D")), false)))
    assert(edges.contains(new Edge("C", new State(Set("C", "E"), Set("D")), true)))
    assert(edges.contains(new Edge("E", new State(Set("C", "E"), Set("D")), true)))
    assert(edges.contains(new Edge("D", new State(Set("C", "E"), Set("D")), false)))

    //check initial edges
    assert(edges.contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(edges.contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(edges.size==14)
  }

  test("Check FindCausalGroups correct functionality - Log 2") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("D"), Set.empty)
    val states = List(
      new State(Set("A"), Set("B", "C")),
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

  test("Check FindCausalGroups correct functionality - Log 3") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("H"), Set.empty)
    val states = List(
      new State(Set("A"), Set("B","C")),
      new State(Set("C"), Set("G")),
      new State(Set("B"), Set("D")),
      new State(Set("D"), Set("F")),
      new State(Set("B"), Set("E")),
      new State(Set("F","G"), Set("H")),
      new State(Set("E"), Set("F"))
    )

    val places = new Places(initialState, finalState, states)

    val findEdges: FindEdges = new FindEdges(places)
    val edges = findEdges.find()

    //check edges
    assert(edges.contains(new Edge("A", new State(Set("A"), Set("B", "C")), true)))
    assert(edges.contains(new Edge("B", new State(Set("A"), Set("B", "C")), false)))
    assert(edges.contains(new Edge("C", new State(Set("A"), Set("B", "C")), false)))
    assert(edges.contains(new Edge("C", new State(Set("C"), Set("G")), true)))
    assert(edges.contains(new Edge("G", new State(Set("C"), Set("G")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B"), Set("D")), true)))
    assert(edges.contains(new Edge("D", new State(Set("B"), Set("D")), false)))
    assert(edges.contains(new Edge("D", new State(Set("D"), Set("F")), true)))
    assert(edges.contains(new Edge("F", new State(Set("D"), Set("F")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B"), Set("E")), true)))
    assert(edges.contains(new Edge("E", new State(Set("B"), Set("E")), false)))
    assert(edges.contains(new Edge("E", new State(Set("E"), Set("F")), true)))
    assert(edges.contains(new Edge("F", new State(Set("E"), Set("F")), false)))
    assert(edges.contains(new Edge("F", new State(Set("F", "G"), Set("H")), true)))
    assert(edges.contains(new Edge("G", new State(Set("F", "G"), Set("H")), true)))
    assert(edges.contains(new Edge("H", new State(Set("F", "G"), Set("H")), false)))

    //check initial edges
    assert(edges.contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(edges.contains(new Edge("H", new State(Set("H"), Set.empty), true)))

    //check wrong directionality
    assert(!edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(edges.size==18)
  }

  test("Check FindCausalGroups correct functionality - Log 4") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("D"), Set.empty)
    val states = List(
      new State(Set("A", "E"), Set("B")),
      new State(Set("B"), Set("C")),
      new State(Set("C"), Set("D", "E"))
    )

    val places = new Places(initialState, finalState, states)

    val findEdges: FindEdges = new FindEdges(places)
    val edges = findEdges.find()

    //check edges
    assert(edges.contains(new Edge("A", new State(Set("A", "E"), Set("B")), true)))
    assert(edges.contains(new Edge("E", new State(Set("A", "E"), Set("B")), true)))
    assert(edges.contains(new Edge("B", new State(Set("A", "E"), Set("B")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B"), Set("C")), true)))
    assert(edges.contains(new Edge("C", new State(Set("B"), Set("C")), false)))
    assert(edges.contains(new Edge("C", new State(Set("C"), Set("D", "E")), true)))
    assert(edges.contains(new Edge("D", new State(Set("C"), Set("D", "E")), false)))
    assert(edges.contains(new Edge("E", new State(Set("C"), Set("D", "E")), false)))


    //check initial edges
    assert(edges.contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(edges.contains(new Edge("D", new State(Set("D"), Set.empty), true)))

    //check wrong directionality
    assert(!edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(edges.size==10)
  }

  test("Check FindCausalGroups correct functionality - Log 5") {
    val initialState = new State(Set.empty, Set("A"))
    val finalState = new State(Set("H"), Set.empty)
    val states = List(
      new State(Set("A"), Set("B","D")),
      new State(Set("B"), Set("C")),
      new State(Set("C","D"), Set("E")),
      new State(Set("E"), Set("F")),
      new State(Set("E"), Set("G")),
      new State(Set("F"), Set("H")),
      new State(Set("G"), Set("H"))
    )

    val places = new Places(initialState, finalState, states)

    val findEdges: FindEdges = new FindEdges(places)
    val edges = findEdges.find()

    //check edges
    assert(edges.contains(new Edge("A", new State(Set("A"), Set("B","D")), true)))
    assert(edges.contains(new Edge("B", new State(Set("A"), Set("B","D")), false)))
    assert(edges.contains(new Edge("D", new State(Set("A"), Set("B","D")), false)))
    assert(edges.contains(new Edge("B", new State(Set("B"), Set("C")), true)))
    assert(edges.contains(new Edge("C", new State(Set("B"), Set("C")), false)))
    assert(edges.contains(new Edge("C", new State(Set("C","D"), Set("E")), true)))
    assert(edges.contains(new Edge("D", new State(Set("C","D"), Set("E")), true)))
    assert(edges.contains(new Edge("E", new State(Set("C","D"), Set("E")), false)))
    assert(edges.contains(new Edge("E", new State(Set("E"), Set("F")), true)))
    assert(edges.contains(new Edge("F", new State(Set("E"), Set("F")), false)))
    assert(edges.contains(new Edge("E", new State(Set("E"), Set("G")), true)))
    assert(edges.contains(new Edge("G", new State(Set("E"), Set("G")), false)))
    assert(edges.contains(new Edge("F", new State(Set("F"), Set("H")), true)))
    assert(edges.contains(new Edge("H", new State(Set("F"), Set("H")), false)))
    assert(edges.contains(new Edge("G", new State(Set("G"), Set("H")), true)))
    assert(edges.contains(new Edge("H", new State(Set("G"), Set("H")), false)))

    //check initial edges
    assert(edges.contains(new Edge("A", new State(Set.empty, Set("A")), false)))

    //check final edges
    assert(edges.contains(new Edge("H", new State(Set("H"), Set.empty), true)))

    //check wrong directionality
    assert(!edges.contains(new Edge("B", new State(Set("B", "C"), Set("D")), false)))
    assert(!edges.contains(new Edge("D", new State(Set("B", "C"), Set("D")), true)))

    assert(edges.size==18)
  }

}
