use cuneiform::cuneiform;
use std::ops::{Deref, DerefMut};

#[cuneiform]
#[derive(Copy, Clone, Default, Debug)]
pub struct CacheAligned<T> {
    pub data: T,
}

impl<T> Deref for CacheAligned<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}

impl<T> DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut T {
        &mut self.data
    }
}

impl<T> CacheAligned<T> {
    pub fn new(t: T) -> Self {
        Self { data: t }
    }
}
