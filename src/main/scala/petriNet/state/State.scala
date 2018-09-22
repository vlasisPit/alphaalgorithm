package petriNet.state

/**
  * States / places (circles on a Petri net) can be input/output of transitions.
  * Places represents the states of a system and transitions represent state changes
  * @param input
  * @param output
  */
@SerialVersionUID(100L)
class State(val input: Set[String], val output: Set[String]) extends Serializable {

  def getInput(): Set[String] = {
    return input
  }

  def getOutput(): Set[String] = {
    return output
  }

  override def toString = s"State($input, $output)"
}
