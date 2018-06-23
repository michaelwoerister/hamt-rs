// Copyright (c) 2013, 2014, 2015, 2016 Michael Woerister
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use std::sync::Arc;

//=-------------------------------------------------------------------------------------------------
// trait ItemStore
//=-------------------------------------------------------------------------------------------------
pub trait ItemStore<K, V>: Clone {
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

impl<K: Clone, V: Clone> ItemStore<K, V> for CopyStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.key }
    fn val<'a>(&'a self) -> &'a V { &self.val }

    fn new(key: K, val: V) -> CopyStore<K, V> {
        CopyStore {
            key: key,
            val: val
        }
    }
}

impl<K: Clone, V: Clone> Clone for CopyStore<K, V> {
    fn clone(&self) -> CopyStore<K, V> {
        CopyStore {
            key: self.key.clone(),
            val: self.val.clone(),
        }
    }
}

unsafe impl <K, V> Send for CopyStore<K, V> where K: Send, V: Send { }

unsafe impl <K, V> Sync for CopyStore<K, V> where K: Sync, V: Sync { }



//=-------------------------------------------------------------------------------------------------
// struct ShareStore
//=-------------------------------------------------------------------------------------------------
pub struct ShareStore<K: Sync + Send, V: Sync+ Send> {
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

unsafe impl <K, V> Send for ShareStore<K, V> where K: Send + Sync, V: Send + Sync { }

unsafe impl <K, V> Sync for ShareStore<K, V> where K: Send + Sync, V: Send + Sync { }
