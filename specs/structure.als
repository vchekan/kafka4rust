abstract sig Object {}
sig File extends Object {}
sig Dir extends Object {
	entries: set Entry
}
one sig Root extends Dir {}
sig Entry {
	name: one Name,
	object: one Object
}
sig Name {}

// facts
fact {
	entries in Dir lone -> Entry
}

fact {
	all d: Dir, disj x, y: d.entries | x.name != y.name
}

fact {
	all d: Dir | lone object.d
}

fact {
	Entry.object = Object - Root
}

fact {
	Entry.object = Object - Root
}


fact {
  // Entries must belong to exactly one a directory
  entries in Dir one -> Entry
}

fact {
   // Directories cannot contain themselves
   all d : Dir | d not in d.entries.object
}

assert no_partitions {
  // Every object is reachable from the root
  Object - Root = Root.^(entries.object)
}

fact {
   // Directories cannot contain themselves directly or indirectly
   all d : Dir | d not in d.^(entries.object)
}

check no_partitions for 6
run example {} for 4
