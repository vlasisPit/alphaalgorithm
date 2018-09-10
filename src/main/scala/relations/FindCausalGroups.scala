package relations

import misc.CausalGroup

/**
  * Let Q,R be set of activities. Then (Q,R) is a causal group iff there is a causal relation -> from each element of Q
  * to each element of R (ie all pairwise combinations of elements of Q and R are in ->) and the members of Q and R are
  * not in ||
  * @tparam T
  */
@SerialVersionUID(100L)
class FindCausalGroups[T] extends Serializable {

  def enrichCausalGroups(directCausalGroups: List[CausalGroup[T]]):List[CausalGroup[T]] = {
    /*return directCausalGroups
        .groupBy(x=>x.group1).toList*/
    return null
  }

}
