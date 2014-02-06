
use extra::arc::Arc;

pub trait ItemStore<K, V>: Clone+Send+Freeze {
    fn key<'a>(&'a self) -> &'a K;
    fn val<'a>(&'a self) -> &'a V;
}

pub struct CopyStore<K, V> {
    key: K,
    val: V
}

impl<K: Clone+Send+Freeze, V: Clone+Send+Freeze> ItemStore<K, V> for CopyStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.key }
    fn val<'a>(&'a self) -> &'a V { &self.val }
}

impl<K: Clone+Send+Freeze, V: Clone+Send+Freeze> Clone for CopyStore<K, V> {
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

impl<K: Send+Freeze, V: Send+Freeze> ShareStore<K, V> {
    pub fn new(k: K, v: V) -> ShareStore<K, V> {
        ShareStore { store: Arc::new((k, v)) }
    }
}

impl<K: Send+Freeze, V: Send+Freeze> ItemStore<K, V> for ShareStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { self.store.get().first_ref() }
    fn val<'a>(&'a self) -> &'a V { self.store.get().second_ref() }
}

impl<K: Send+Freeze, V: Send+Freeze> Clone for ShareStore<K, V> {
    fn clone(&self) -> ShareStore<K, V> {
        ShareStore {
            store: self.store.clone()
        }
    }
}