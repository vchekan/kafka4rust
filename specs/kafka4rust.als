sig Msg {}
sig Partition {
	var queue: set Msg
}

// at least one partition
fact { some Partition }

// initial queues are empty
fact { no queue }

// Msg belong only to single partition
//fact {
	//queue in Partition one -> set Msg
//}

//
// Message flow
//

pred stutter {
	queue' = queue
}

pred msg_add[p: Partition, m: Msg] {
	p.queue' = p.queue + m
}

fact {
	always (
		stutter or
		some p: Partition, m: Msg | msg_add[p, m]
	)
}

run example {} for 3 Partition, exactly 4 Msg

