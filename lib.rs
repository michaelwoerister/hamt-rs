// Copyright (c) 2013, 2014 Michael Woerister
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

//! This library contains some implementations of persistent data structures. Persistent
//! data structures, also called purely functional data structures, are always immutable from the
//! view of the user and can safely and easily be shared in a concurrent setting. For more
//! information see [Wikipedia](https://en.wikipedia.org/wiki/Persistent_data_structure) for
//! example.

#![feature(default_type_params)]
#![feature(macro_rules)]
#![allow(deprecated_owned_vector)]

#[cfg(test)]
extern crate test;
#[cfg(test)]
extern crate collections;
#[cfg(test)]
extern crate rand;

extern crate sync;

use std::hash::sip::SipHasher;

pub use hamt::HamtMapIterator;

mod hamt;
mod item_store;

#[cfg(test)]
mod testing;

#[cfg(test)]
mod rbtree;


/// A trait to represent persistent maps. Objects implementing this trait are supposed to be
/// cheaply copyable. Typically they can be seen as a kind of smart pointer with similar performance
/// characteristics.
pub trait PersistentMap<K, V>: Map<K, V> + Clone {
    /// Inserts a key-value pair into the map. An existing value for a
    /// key is replaced by the new value. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn insert(self, key: K, value: V) -> (Self, bool);

    /// Removes a key-value pair from the map. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn remove(self, key: &K) -> (Self, bool);

    /// Inserts a key-value pair into the map. Same as `insert()` but with a return type that's
    /// better suited to chaining multiple calls together.
    fn plus(self, key: K, val: V) -> Self {
        self.insert(key, val).val0()
    }

    /// Removes a key-value pair from the map. Same as `remove()` but with a return type that's
    /// better suited to chaining multiple call together
    fn minus(self, key: &K) -> Self {
        self.remove(key).val0()
    }
}

pub type HamtMap<K, V, H=SipHasher> = hamt::HamtMap<K, V, item_store::ShareStore<K, V>, H>;
pub type CloningHamtMap<K, V, H=SipHasher> = hamt::HamtMap<K, V, item_store::CopyStore<K, V>, H>;


/// A trait to represent persistent sets. Objects implementing this trait are supposed to be
/// cheaply copyable. Typically they can be seen as a kind of smart pointer with similar performance
/// characteristics.
pub trait PersistentSet<K>: Set<K> + Clone {
    /// Inserts a value into the set. The first tuple element of the return value is the new
    /// map instance representing the set after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn insert(self, value: K) -> (Self, bool);

    /// Removes a value from the set. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn remove(self, key: &K) -> (Self, bool);

    /// Inserts a value into the set. Same as `insert()` but with a return type that's
    /// better suited to chaining multiple calls together.
    fn plus(self, key: K) -> Self {
        self.insert(key).val0()
    }

    /// Removes a value from the set. Same as `remove()` but with a return type that's
    /// better suited to chaining multiple call together
    fn minus(self, key: &K) -> Self {
        self.remove(key).val0()
    }
}
