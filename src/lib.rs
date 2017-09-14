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

//! This library contains some implementations of persistent data structures. Persistent
//! data structures, also called purely functional data structures, are always immutable from the
//! view of the user and can safely and easily be shared in a concurrent setting. For more
//! information see [Wikipedia](https://en.wikipedia.org/wiki/Persistent_data_structure) for
//! example.

#![allow(unused_parens)]

extern crate libc;

pub use hamt::HamtMap;
pub use hamt::HamtMapIterator;
pub use item_store::{ItemStore, ShareStore, CopyStore};

mod hamt;
mod item_store;

#[cfg(test)]
mod testing;
