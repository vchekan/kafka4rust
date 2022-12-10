use bytes::BytesMut;

// TODO: implement real pool
pub(crate) fn get() -> BytesMut {
    // TODO: evaluate either BytesMut have any benefits
    // TODO: dynamic growth
    BytesMut::with_capacity(512*1024)
}