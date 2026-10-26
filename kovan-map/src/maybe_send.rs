//! What the maps ask of a key or value so that freeing it on another thread is sound.

/// `Send` on every target where threads can share memory, which is every target but wasm32
/// without the `atomics` feature. A replaced or removed entry is freed on whichever thread frees
/// its batch, so there its key and value must be `Send`.
#[cfg(not(all(target_arch = "wasm32", not(target_feature = "atomics"))))]
pub trait MaybeSend: Send {}

#[cfg(not(all(target_arch = "wasm32", not(target_feature = "atomics"))))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// Nothing on wasm32 without the `atomics` feature: the program has one thread, every entry is
/// freed on it, so a map may hold keys and values that are not `Send` (an `Rc`).
#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
pub trait MaybeSend {}

#[cfg(all(target_arch = "wasm32", not(target_feature = "atomics")))]
impl<T: ?Sized> MaybeSend for T {}
