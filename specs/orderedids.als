module ordered[A]

sig Aux in A {
  next : lone Aux
}
fact {
  Aux = A
}
one sig first, last in A {}

fact {
  // A is totally ordered with next
  no last.next
  A in first.*next
}
