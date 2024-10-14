use std::{marker::PhantomData, ops::{Deref, DerefMut}};

use bytes::BytesMut;

/// BytesMut wraper which preserves protocol type.
/// This allows to pass response type to deserializer.
/// To expose underlying buffer, use deref: `*` operator.
pub(crate) struct TypedBuffer<T>(BytesMut, PhantomData<T>);

impl <T> TypedBuffer<T> {
    pub fn new(buff: BytesMut) -> Self {
        Self(buff, PhantomData)
    }

    pub fn unwrap(self) -> BytesMut {
        self.0
    }
}

impl <T> Deref for TypedBuffer<T> {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl <T> DerefMut for TypedBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}