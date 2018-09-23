package petriNet.state


/**
  * States / places (circles on a Petri net) can be input/output of transitions.
  * Places represents the states of a system and transitions represent state changes
  *
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

  def canEqual(a: Any) = a.isInstanceOf[State]

  override def equals(that: Any): Boolean =
    that match {
      case that: State => that.canEqual(this) &&
        this.getInput() == that.getInput() &&
        this.getOutput() == that.getOutput() &&
        this.hashCode == that.hashCode
      case _ => false
    }

  override def hashCode: Int = {
    val prime = 31
    var result = 1
    result = prime * result + input.hashCode()
    result = prime * result + output.hashCode()
    return result
  }

  override def toString = s"State($input, $output)"
}
