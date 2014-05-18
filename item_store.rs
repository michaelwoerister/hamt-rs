
use sync::Arc;

pub trait ItemStore<K, V>: Clone+Send+Share {
    fn key<'a>(&'a self) -> &'a K;
    fn val<'a>(&'a self) -> &'a V;
}

pub struct CopyStore<K, V> {
    key: K,
    val: V
}

impl<K: Clone+Send+Share, V: Clone+Send+Share> CopyStore<K, V> {
    pub fn new(key: K, val: V) -> CopyStore<K, V> {
        CopyStore {
            key: key,
            val: val
        }
    }
}

impl<K: Clone+Send+Share, V: Clone+Send+Share> ItemStore<K, V> for CopyStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.key }
    fn val<'a>(&'a self) -> &'a V { &self.val }
}

impl<K: Clone+Send+Share, V: Clone+Send+Share> Clone for CopyStore<K, V> {
    fn clone(&self) -> CopyStore<K, V> {
        CopyStore {
            key: self.key.clone(),
            val: self.val.clone(),
        }
    }
}

pub struct ShareStore<K, V> {
    store: Arc<(K, V)>,
}

impl<K: Send+Share, V: Send+Share> ShareStore<K, V> {
    pub fn new(k: K, v: V) -> ShareStore<K, V> {
        ShareStore { store: Arc::new((k, v)) }
    }
}

impl<K: Send+Share, V: Send+Share> ItemStore<K, V> for ShareStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { self.store.ref0() }
    fn val<'a>(&'a self) -> &'a V { self.store.ref1() }
}

impl<K: Send+Share, V: Send+Share> Clone for ShareStore<K, V> {
    fn clone(&self) -> ShareStore<K, V> {
        ShareStore {
            store: self.store.clone()
        }
    }
}