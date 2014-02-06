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

#[feature(macro_rules)];

extern mod extra;
extern mod sync;

pub mod hamt;
pub mod rbtree;

mod item_store;
mod test;

/// A trait to represent persistent maps. Objects implementing this trait are supposed to be
/// cheaply copyable. Typically they can be seen as a kind of smart pointer with similar performance
/// characteristics.
pub trait PersistentMap<K: Send+Freeze, V: Send+Freeze>: Map<K, V> + Clone {
    /// Inserts a key-value pair into the map. An existing value for a
    /// key is replaced by the new value. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn insert(self, key: K, value: V) -> (Self, bool);

    /// Removes a key-value pair from the map. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    fn remove(self, key: &K) -> (Self, bool);
}
