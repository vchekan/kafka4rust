//open ordered[Id]
open util/ordering[Node]

sig Node {
  succ : one Node,
  id : one Id,
  var inbox: set Id,
  var outbox: set Id
}

var sig Elected in Node {}

sig Id {}

fact {
  // at least one node
  some Node
}

fact {
  // succ forms a ring
  all n : Node | Node in n.^succ
}

fact {
  // ids are unique
  all i : Id | lone id.i
}

fact {
  // initially inbox and outbox are empty
  no inbox and no outbox
  // initially there are no elected nodes
  no Elected
}

pred initiate[n: Node] {
	n.outbox' = n.outbox + n.id
	all m: Node - n | m.outbox' = m.outbox

	inbox' = inbox
	Elected' = Elected
}

assert AtMostOneLeader {
	always (lone Elected)
}

assert AtLeastOneLeader {
  eventually (some Elected)
}

check AtMostOneLeader //4 but 20 steps
check AtLeastOneLeader

run example {} //for exactly 3 Node, exactly 3 Id
