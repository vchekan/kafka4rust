var sig File {}
var sig Trash in File {}

fact init {
	no Trash
}

pred empty {
	some Trash
	after no Trash
	File' = File - Trash
}

pred delete [f: File] {
	not (f in Trash)
	Trash' = Trash + f
	File' = File
}

pred restore [f: File] {
	f in Trash
	Trash' = Trash - f
	File' = File + f
}

pred do_non_detete {
  File' = File
  Trash' = Trash
}

fact trans {
	always (empty or (some f: File | delete[f] or restore[f]) or do_non_detete)
}

assert restore_after_delete {
	always (all f: File | restore[f] implies once delete[f])
}


assert delete_all {
	always ((Trash = File and empty) implies after always no File)
}

check delete_all
check restore_after_delete for 5 but 1.. steps


