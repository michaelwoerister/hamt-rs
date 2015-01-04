
use std::sync::Arc;

//=-------------------------------------------------------------------------------------------------
// trait ItemStore
//=-------------------------------------------------------------------------------------------------
pub trait ItemStore<K, V>: Clone+Send+Sync {
    fn key<'a>(&'a self) -> &'a K;
    fn val<'a>(&'a self) -> &'a V;

    fn new(key: K, val: V) -> Self;
}



//=-------------------------------------------------------------------------------------------------
// struct CopyStore
//=-------------------------------------------------------------------------------------------------
pub struct CopyStore<K, V> {
    key: K,
    val: V
}

impl<K: Clone+Send+Sync, V: Clone+Send+Sync> ItemStore<K, V> for CopyStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.key }
    fn val<'a>(&'a self) -> &'a V { &self.val }

    fn new(key: K, val: V) -> CopyStore<K, V> {
        CopyStore {
            key: key,
            val: val
        }
    }
}

impl<K: Clone+Send+Sync, V: Clone+Send+Sync> Clone for CopyStore<K, V> {
    fn clone(&self) -> CopyStore<K, V> {
        CopyStore {
            key: self.key.clone(),
            val: self.val.clone(),
        }
    }
}



//=-------------------------------------------------------------------------------------------------
// struct ShareStore
//=-------------------------------------------------------------------------------------------------
pub struct ShareStore<K, V> {
    store: Arc<(K, V)>,
}

impl<K: Send+Sync, V: Send+Sync> ItemStore<K, V> for ShareStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.store.0 }
    fn val<'a>(&'a self) -> &'a V { &self.store.1 }

    fn new(k: K, v: V) -> ShareStore<K, V> {
        ShareStore { store: Arc::new((k, v)) }
    }
}

impl<K: Send+Sync, V: Send+Sync> Clone for ShareStore<K, V> {
    fn clone(&self) -> ShareStore<K, V> {
        ShareStore {
            store: self.store.clone()
        }
    }
}
