Metadata update can happen when `&mut self` is accessible. This can be achieved either by mutex or by handling all
updates in `Poll`. Which means that we have to listen to all events in single place, `Poll` or `select!` macro.

Stashed request.
When awaiting for buffer to become available, we need to keep listening for metadata update events, 

Select DIY?
Can I write a `Poll` which would implement `select` in specific manner? Can I address dropped future problem?