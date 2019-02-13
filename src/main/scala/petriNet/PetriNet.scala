package petriNet

import petriNet.flow.Edge
import petriNet.state.Places

@SerialVersionUID(100L)
class PetriNet (val places: Places, val events: List[String], val edges: List[Edge]) extends Serializable {

  def getPlaces(): Places = {
    return places
  }

  def getEvents(): List[String] = {
    return events
  }

  def getEdges() : List[Edge] = {
    return edges
  }


  override def toString = s"PetriNet(\n places = \t  $getPlaces \n events = \t  $getEvents, \n edges = \t  $getEdges)"
}
