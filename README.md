# Alpha Algorithm - Process Mining Algorithm
Alpha algorithm is one of the first Process Mining algorithms that discovers Workflow Nets (in form of Petri Nets) from logs (traces).  
In order to discover a workflow net from logs, we need to establish the ordering between the transitions of this workflow. These relations will later be used in order to find places and connections between the transitions and these places.  
We define the following relations between transitions in the log  

* direct succession x>y (x>y we see in log sub-traces ...xy...)
* causality x->y (i.e. if there are traces ...xy... and no traces ...yx...)
* parallel x || y (i.e. can see both ...xy... and ...yx...)
* unrelated x # y (i.e. there are no traces ...xy... nor ...yx...)

The set of all relations for a log L is called the footprint of L  

With these relations we define the Alpha algorithm as follows:  

* extract all transition names from L to set T
* let Ti be the set of all initial transitions and To the set of all end transitions
* find all causal pairs of sets (A,B). Let A,B be two sets of activities. Then (A,B) is a causal group iff there is a causal relation -> from each element of A to each element of B (ie all pairwise combinations of elements of A and B are in ->) and the members of A and B are not in || relation.
* once found all such sets, we retain only the maximal ones 
* for each such pair (A,B) we connect all elements from A with all elements from B with one single place p(A,B)
* then we also connect appropriate transitions with the input and output places
* finally we connect the start place i to all transitions from Ti
* and all transitions from To with the final state o  

# Implementation
Alpha algorithm is implemented in this project using Apache Spark as distributed data processing engine. Scala used to write the code of the algorithm.
The implementation is divided into the following steps.

* Step 0 - Extract and filter out traces 								 
* Step 1 - Find all transitions / events, Sorted list of all event types 
* Step 2 - Construct a set with all start activities (Ti)				 
* Step 3 - Construct a set with all final activities (To)				 
* Step 4 - Footprint graph - Causal groups								 
* Step 4 - Causal groups												 
* Step 5 - compute only maximal groups									 
* step 6 - set of places/states											 
* step 7 - set of arcs (flow)											 
* step 8 - construct petri net											 

# Testing
Impementation of the algorithm is tested using unit tests and Databricks Community edition environment.
