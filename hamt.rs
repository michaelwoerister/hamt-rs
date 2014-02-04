// Hash Array Mapped Trie Implementation
// Based on "Ideal Hash Trees" by Phil Bagwell:
// http://lampwww.epfl.ch/papers/idealhashtrees.pdf

// Copyright (c) 2013 Michael Woerister
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

use std::cast;
use std::mem;
use std::ptr;
use std::vec;
use std::unstable::intrinsics;
use std::sync::atomics::{AtomicUint, Acquire, Release};
use std::rt::global_heap::{exchange_malloc, exchange_free};

use extra::arc::Arc;
use persistent::PersistentMap;
use item_store::{ItemStore, CopyStore, ShareStore};

static BITS_PER_LEVEL: uint = 5;
static LAST_LEVEL: uint = (64 / BITS_PER_LEVEL) - 1;
static LEVEL_BIT_MASK: u64 = (1 << BITS_PER_LEVEL) - 1;
static MIN_CAPACITY: uint = 8;

enum NodeEntry<K, V, IS> {
    Collision(Arc<~[IS]>),
    SingleItem(IS),
    SubTree(NodeRef<K, V, IS>)
}

impl<K:Hash + Eq + Send + Freeze, V: Send + Freeze, IS: ItemStore<K, V> + Send + Freeze> Clone for NodeEntry<K, V, IS> {
    fn clone(&self) -> NodeEntry<K, V, IS> {
        match *self {
            Collision(ref items) => Collision(items.clone()),
            SingleItem(ref kvp) => SingleItem(kvp.clone()),
            SubTree(ref sub) => SubTree(sub.clone()),
        }
    }
}

struct UnsafeNode<K, V, IS> {
    ref_count: AtomicUint,
    mask: u32,
    capacity: u8,
    __entries: [NodeEntry<K, V, IS>, ..0],
}

impl<K, V, IS> UnsafeNode<K, V, IS> {
    #[inline]
    fn get_entry<'a>(&'a self, index: uint) -> &'a NodeEntry<K, V, IS> {
        unsafe {
            assert!(index < self.entry_count());
            let base: *NodeEntry<K, V, IS> = cast::transmute(&self.__entries);
            let entry_ptr = base.offset(index as int);
            cast::transmute(entry_ptr)
        }
    }

    #[inline]
    fn get_entry_mut<'a>(&'a mut self, index: uint) -> &'a mut NodeEntry<K, V, IS> {
        unsafe {
            assert!(index < self.entry_count());
            let base: *NodeEntry<K, V, IS> = cast::transmute(&self.__entries);
            let entry_ptr = base.offset(index as int);
            cast::transmute(entry_ptr)
        }
    }

    #[inline]
    fn init_entry<'a>(&mut self, index: uint, entry: NodeEntry<K, V, IS>) {
        unsafe {
            assert!(index < self.entry_count());
            let base: *mut NodeEntry<K, V, IS> = cast::transmute(&self.__entries);
            let entry_ptr: &mut NodeEntry<K, V, IS> = cast::transmute(base.offset(index as int));
            intrinsics::move_val_init(entry_ptr, entry);
        }
    }

    #[inline]
    fn entry_count(&self) -> uint {
        bit_count(self.mask)
    }

    fn alloc(mask: u32, capacity: uint) -> NodeRef<K, V, IS> {
        fn size_of_zero_entry_array<K, V, IS>() -> uint {
            let node: UnsafeNode<K, V, IS> = UnsafeNode {
                ref_count: AtomicUint::new(0),
                mask: 0,
                capacity: 0,
                __entries: [],
            };

            mem::size_of_val(&node.__entries)
        }
        #[inline]
        fn align_to(size: uint, align: uint) -> uint {
            assert!(align != 0 && bit_count(align as u32) == 1);
            (size + align - 1) & !(align - 1)
        }

        let align = mem::pref_align_of::<NodeEntry<K, V, IS>>();

        assert!(size_of_zero_entry_array::<K, V, IS>() == 0);
        assert!(bit_count(mask) <= capacity);
        let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS>>(), align);

        let node_size = header_size + capacity * mem::size_of::<NodeEntry<K, V, IS>>();

        unsafe {
            let node_ptr: *mut UnsafeNode<K, V, IS> = cast::transmute(exchange_malloc(node_size));
            intrinsics::move_val_init(&mut (*node_ptr).ref_count, AtomicUint::new(1));
            intrinsics::move_val_init(&mut (*node_ptr).mask, mask);
            intrinsics::move_val_init(&mut (*node_ptr).capacity, capacity as u8);
            NodeRef { ptr: node_ptr }
        }
    }

    fn destroy(&self) {
        unsafe {
            let entry_count = self.entry_count();

            for i in range(0, entry_count) {
                self.drop_entry(i)
            }

            exchange_free(cast::transmute(self));
        }
    }

    unsafe fn drop_entry(&self, index: uint) {
        // destroy the contained object, trick from Rc
        let _ = ptr::read_ptr(self.get_entry(index));
    }
}

struct NodeRef<K, V, IS> {
    ptr: *mut UnsafeNode<K, V, IS>
}

enum NodeRefBorrowResult<'a, K, V, IS> {
    OwnedNode(&'a mut UnsafeNode<K, V, IS>),
    SharedNode(&'a UnsafeNode<K, V, IS>),
}

impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V>> NodeRef<K, V, IS> {
    #[inline]
    fn borrow<'a>(&'a self) -> &'a UnsafeNode<K, V, IS> {
        unsafe {
            cast::transmute(self.ptr)
        }
    }

    fn borrow_mut<'a>(&'a mut self) -> &'a mut UnsafeNode<K, V, IS> {
        unsafe {
            assert!((*self.ptr).ref_count.load(Acquire) == 1);
            cast::transmute(self.ptr)
        }
    }

    fn try_borrow_owned<'a>(&'a mut self) -> NodeRefBorrowResult<'a, K, V, IS> {
        unsafe {
            if (*self.ptr).ref_count.load(Acquire) == 1 {
                OwnedNode(cast::transmute(self.ptr))
            } else {
                SharedNode(cast::transmute(self.ptr))
            }
        }
    }
}

#[unsafe_destructor]
impl<K, V, IS> Drop for NodeRef<K, V, IS> {
    fn drop(&mut self) {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS> = cast::transmute(self.ptr);
            let old_count = node.ref_count.fetch_sub(1, Acquire);
            assert!(old_count >= 1);
            if old_count == 1 {
                node.destroy()
            }
        }
    }
}

impl<K, V, IS> Clone for NodeRef<K, V, IS> {
    fn clone(&self) -> NodeRef<K, V, IS> {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS> = cast::transmute(self.ptr);
            let old_count = node.ref_count.fetch_add(1, Release);
            assert!(old_count >= 1);
        }

        NodeRef { ptr: self.ptr }
    }
}

enum RemovalResult<K, V, IS> {
    // Don't do anything
    NoChange,
    // Replace the sub-tree entry with another sub-tree entry pointing to the given node
    ReplaceSubTree(NodeRef<K, V, IS>),
    // Collapse the sub-tree into a singe-item entry
    CollapseSubTree(IS),
    // Completely remove the entry
    KillSubTree
}

impl<K:Hash + Eq + Send + Freeze, V: Send + Freeze, IS: ItemStore<K, V>> UnsafeNode<K, V, IS> {
    fn insert(&self,
              hash: u64,
              level: uint,
              new_kvp: IS,
              insertion_count: &mut uint)
           -> NodeRef<K, V, IS> {
        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            return self.copy_with_new_entry(local_key, SingleItem(new_kvp));
        }

        let index = get_index(self.mask, local_key);

        match *self.get_entry(index) {
            SingleItem(ref existing_entry) => {
                let existing_key = existing_entry.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    self.copy_with_new_entry(local_key, SingleItem(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = existing_key.hash() >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_entry,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    self.copy_with_new_entry(local_key, SubTree(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = ~[new_kvp, existing_entry.clone()];
                    let collision_entry = Collision(Arc::new(items));
                    self.copy_with_new_entry(local_key, collision_entry)
                }
            }
            Collision(ref items) => {
                assert!(level == LAST_LEVEL);
                let items = items.get();
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.push_all(items.as_slice());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = vec::with_capacity(item_count);

                        if position > 0 {
                            new_items.push_all(items.slice_to(position));
                        }

                        new_items.push(new_kvp);

                        if position < item_count - 1 {
                           new_items.push_all(items.slice_from(position + 1));
                        }

                        assert!(new_items.len() == item_count);
                        new_items
                    }
                };

                self.copy_with_new_entry(local_key, Collision(Arc::new(new_items)))
            }
            SubTree(ref sub_tree) => {
                let new_sub_tree = sub_tree.borrow().insert(hash >> BITS_PER_LEVEL,
                                                            level + 1,
                                                            new_kvp,
                                                            insertion_count);

                self.copy_with_new_entry(local_key, SubTree(new_sub_tree))
            }
        }
    }

    fn try_insert_in_place(&mut self,
                           hash: u64,
                           level: uint,
                           new_kvp: IS,
                           insertion_count: &mut uint)
                        -> bool {
        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if !self.can_insert_in_place(local_key) {
            return false;
        }

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            self.insert_entry_in_place(local_key, SingleItem(new_kvp));
            return true;
        }

        let index = get_index(self.mask, local_key);

        let xxx = match *self.get_entry_mut(index) {
            SingleItem(ref existing_entry) => {
                let existing_key = existing_entry.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    //self.insert_entry_in_place(local_key, SingleItem(new_kvp));
                    Some(SingleItem(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = existing_key.hash() >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_entry,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    // self.copy_with_new_entry(local_key, SubTree(new_sub_tree))
                    //self.insert_entry_in_place(local_key, SubTree(new_sub_tree));
                    Some(SubTree(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = ~[new_kvp, existing_entry.clone()];
                    let collision_entry = Collision(Arc::new(items));
                    //self.copy_with_new_entry(local_key, collision_entry)
                    // self.insert_entry_in_place(local_key, collision_entry);
                    Some(collision_entry)
                }
            }
            Collision(ref items) => {
                assert!(level == LAST_LEVEL);
                let items = items.get();
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.push_all(items.as_slice());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = vec::with_capacity(item_count);

                        if position > 0 {
                            new_items.push_all(items.slice_to(position));
                        }

                        new_items.push(new_kvp);

                        if position < item_count - 1 {
                           new_items.push_all(items.slice_from(position + 1));
                        }

                        assert!(new_items.len() == item_count);
                        new_items
                    }
                };

                //self.copy_with_new_entry(local_key, Collision(Arc::new(new_items)))
                //self.insert_entry_in_place(local_key, Collision(Arc::new(new_items)));
                Some(Collision(Arc::new(new_items)))
            }
            SubTree(ref mut subtree_ref) => {
                match subtree_ref.try_borrow_owned() {
                    SharedNode(subtree) => {
                        Some(SubTree(subtree.insert(hash >> BITS_PER_LEVEL,
                                                         level + 1,
                                                         new_kvp,
                                                         insertion_count)))
                    }
                    OwnedNode(subtree) => {
                        if !subtree.try_insert_in_place(hash >> BITS_PER_LEVEL,
                                                        level + 1,
                                                        new_kvp.clone(),
                                                        insertion_count) {
                            Some(SubTree(subtree.insert(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          new_kvp,
                                                          insertion_count)))
                        } else {
                            None
                        }
                    }
                }
            }
        };

        match xxx {
            Some(e) => {
                self.insert_entry_in_place(local_key, e);
            }
            None => {}
        }

        return true;
    }

    fn remove(&self,
              hash: u64,
              level: uint,
              key: &K,
              removal_count: &mut uint)
           -> RemovalResult<K, V, IS> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if (self.mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return NoChange;
        }

        let index = get_index(self.mask, local_key);

        match *self.get_entry(index) {
            SingleItem(ref existing_kvp) => {
                if *existing_kvp.key() == *key {
                    *removal_count = 1;
                    self.collapse_kill_or_change(local_key, index)
                } else {
                    *removal_count = 0;
                    NoChange
                }
            }
            Collision(ref items) => {
                assert!(level == LAST_LEVEL);
                let items = items.get();
                let position = items.iter().position(|kvp| *kvp.key() == *key);

                match position {
                    None => {
                        *removal_count = 0;
                        NoChange
                    },
                    Some(position) => {
                        *removal_count = 1;
                        let item_count = items.len() - 1;

                        // The new entry can either still be a collision node, or it can be a simple
                        // single-item node if the hash collision has been resolved by the removal
                        let new_entry = if item_count > 1 {
                            let mut new_items = vec::with_capacity(item_count);

                            if position > 0 {
                                new_items.push_all(items.slice_to(position));
                            }
                            if position < item_count - 1 {
                                new_items.push_all(items.slice_from(position + 1));
                            }
                            assert!(new_items.len() == item_count);

                            Collision(Arc::new(new_items))
                        } else {
                            assert!(items.len() == 2);
                            assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = items[index_of_remaining_item].clone();

                            SingleItem(kvp)
                        };

                        let new_sub_tree = self.copy_with_new_entry(local_key, new_entry);
                        ReplaceSubTree(new_sub_tree)
                    }
                }
            }
            SubTree(ref sub_tree) => {
                let result = sub_tree.borrow().remove(hash >> BITS_PER_LEVEL,
                                                      level + 1,
                                                      key,
                                                      removal_count);
                match result {
                    NoChange => NoChange,
                    ReplaceSubTree(x) => {
                        ReplaceSubTree(self.copy_with_new_entry(local_key, SubTree(x)))
                    }
                    CollapseSubTree(kvp) => {
                        if bit_count(self.mask) == 1 {
                            CollapseSubTree(kvp)
                        } else {
                            ReplaceSubTree(self.copy_with_new_entry(local_key, SingleItem(kvp)))
                        }
                    },
                    KillSubTree => {
                        self.collapse_kill_or_change(local_key, index)
                    }
                }
            }
        }
    }

    // Determines how the parent node should handle the removal of the entry at local_key from this
    // node.
    fn collapse_kill_or_change(&self, local_key: uint, entry_index: uint) -> RemovalResult<K, V, IS> {
        let next_entry_count = bit_count(self.mask) - 1;

        if next_entry_count > 1 {
            ReplaceSubTree(self.copy_without_entry(local_key))
        } else if next_entry_count == 1 {
            let other_index = 1 - entry_index;

            match *self.get_entry(other_index) {
                SingleItem(ref kvp) => {
                    CollapseSubTree(kvp.clone())
                }
                _ => ReplaceSubTree(self.copy_without_entry(local_key))
            }
        } else {
            assert!(next_entry_count == 0);
            KillSubTree
        }
    }

    fn copy_with_new_entry(&self,
                           local_key: uint,
                           new_entry: NodeEntry<K, V, IS>)
                        -> NodeRef<K, V, IS> {
        let replace_old_entry = (self.mask & (1 << local_key)) != 0;
        let new_mask: u32 = self.mask | (1 << local_key);
        let mut new_node_ref = UnsafeNode::alloc(new_mask, self.expanded_capacity());

        {
            let new_node = new_node_ref.borrow_mut();

            let index = get_index(new_mask, local_key);

            let mut old_i = 0;
            let mut new_i = 0;

            // Copy up to index
            while old_i < index {
                new_node.init_entry(new_i, self.get_entry(old_i).clone());
                old_i += 1;
                new_i += 1;
            }

            // Add new entry
            new_node.init_entry(new_i, new_entry);
            new_i += 1;

            if replace_old_entry {
                // Skip the replaced value
                old_i += 1;
            }

            // Copy the rest
            while old_i < self.entry_count() {
                new_node.init_entry(new_i, self.get_entry(old_i).clone());
                old_i += 1;
                new_i += 1;
            }

            assert!(new_i == bit_count(new_mask));
        }

        return new_node_ref;
    }

    fn can_insert_in_place(&self, local_key: uint) -> bool {
        let bit = (1 << local_key);
        if (self.mask & bit) != 0 {
            true
        } else {
            self.entry_count() < (self.capacity as uint)
        }
    }

    fn insert_entry_in_place(&mut self,
                             local_key: uint,
                             new_entry: NodeEntry<K, V, IS>) {
        let new_mask: u32 = self.mask | (1 << local_key);
        let replace_old_entry = (new_mask == self.mask);
        let index = get_index(new_mask, local_key);

        if replace_old_entry {
            // Destroy the replaced entry
            unsafe {
                self.drop_entry(index);
                self.init_entry(index, new_entry);
            }
        } else {
            assert!(self.capacity as uint > self.entry_count());
            // make place for new entry:
            unsafe {
                if index < self.entry_count() {
                    let source = ptr::to_unsafe_ptr(self.get_entry(index));
                    let dest = cast::transmute_mut_unsafe(source.offset(1));
                    let count = self.entry_count() - index;
                    ptr::copy_memory(dest, source, count);
                }
            }

            self.mask = new_mask;
            self.init_entry(index, new_entry);
        }
    }

    fn expanded_capacity(&self) -> uint {
        if self.capacity == 0 {
            MIN_CAPACITY
        } else if self.capacity > 16 {
            32
        } else {
            ((self.capacity as uint) * 2)
        }
    }

    fn copy_without_entry(&self, local_key: uint) -> NodeRef<K, V, IS> {
        assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let mut new_node_ref = UnsafeNode::alloc(new_mask, self.expanded_capacity());
        {
            let new_node = new_node_ref.borrow_mut();
            let index = get_index(self.mask, local_key);

            let mut old_i = 0;
            let mut new_i = 0;

            // Copy up to index
            while old_i < index {
                new_node.init_entry(new_i, self.get_entry(old_i).clone());
                old_i += 1;
                new_i += 1;
            }

            old_i += 1;

            // Copy the rest
            while old_i < self.entry_count() {
                new_node.init_entry(new_i, self.get_entry(old_i).clone());
                old_i += 1;
                new_i += 1;
            }

            assert!(new_i == bit_count(new_mask));
        }
        return new_node_ref;
    }

    fn remove_entry_in_place(&mut self, local_key: uint) {
        assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let index = get_index(self.mask, local_key);

        unsafe {
            self.drop_entry(index);

            let source = ptr::to_unsafe_ptr(self.get_entry(index + 1));
            let dest = cast::transmute_mut_unsafe(source.offset(-1));
            let count = self.entry_count() - (index + 1);
            ptr::copy_memory(dest, source, count);
        }

        self.mask = new_mask;
    }

    fn new_with_entries(new_kvp: IS,
                        new_hash: u64,
                        existing_kvp: &IS,
                        existing_hash: u64,
                        level: uint)
                     -> NodeRef<K, V, IS> {
        assert!(level <= LAST_LEVEL);

        let new_local_key = new_hash & LEVEL_BIT_MASK;
        let existing_local_key = existing_hash & LEVEL_BIT_MASK;

        if new_local_key != existing_local_key {
            let mask = (1 << new_local_key) | (1 << existing_local_key);
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();

                if new_local_key < existing_local_key {
                    new_node.init_entry(0, SingleItem(new_kvp));
                    new_node.init_entry(1, SingleItem(existing_kvp.clone()));
                } else {
                    new_node.init_entry(0, SingleItem(existing_kvp.clone()));
                    new_node.init_entry(1, SingleItem(new_kvp));
                };
            }
            new_node_ref
        } else if level == LAST_LEVEL {
            let mask = 1 << new_local_key;
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();
                new_node.init_entry(0, Collision(Arc::new(~[new_kvp, existing_kvp.clone()])));
            }
            new_node_ref
        } else {
            // recurse further
            let sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                        new_hash >> BITS_PER_LEVEL,
                                                        existing_kvp,
                                                        existing_hash >> BITS_PER_LEVEL,
                                                        level + 1);
            let mask = 1 << new_local_key;
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();
                new_node.init_entry(0, SubTree(sub_tree));
            }
            new_node_ref
        }
    }
}

struct HamtMap<K, V, IS> {
    root: NodeRef<K, V, IS>,
    element_count: uint,
}

// Clone
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V> + Send + Freeze> Clone for HamtMap<K, V, IS> {
    fn clone(&self) -> HamtMap<K, V, IS> {
        HamtMap { root: self.root.clone(), element_count: self.element_count }
    }
}

// Container
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V>> Container for HamtMap<K, V, IS> {
    fn len(&self) -> uint {
        self.element_count
    }
}

impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V> + Send + Freeze> HamtMap<K, V, IS> {

    fn new() -> HamtMap<K, V, IS> {
        HamtMap {
            root: UnsafeNode::alloc(0, 0),
            element_count: 0
        }
    }

    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let mut hash = key.hash();

        let mut level = 0;
        let mut current_node = &self.root;

        loop {
            assert!(level <= LAST_LEVEL);
            let local_key = (hash & LEVEL_BIT_MASK) as uint;

            if (current_node.borrow().mask & (1 << local_key)) == 0 {
                return None;
            }

            let index = get_index(current_node.borrow().mask, local_key);

            match *current_node.borrow().get_entry(index) {
                SingleItem(ref kvp) => return if *key == *kvp.key() {
                    Some(kvp.val())
                } else {
                    None
                },
                Collision(ref items) => {
                    assert!(level == LAST_LEVEL);
                    let found = items.get().iter().find(|&kvp| *key == *kvp.key());
                    return match found {
                        Some(kvp) => Some(kvp.val()),
                        None => None,
                    };
                }
                SubTree(ref sub) => {
                    assert!(level < LAST_LEVEL);
                    current_node = sub;
                    hash = hash >> BITS_PER_LEVEL;
                    level += 1;
                }
            };
        }
    }

    fn insert(mut self, kvp: IS) -> (HamtMap<K, V, IS>, bool) {
        let hash = kvp.key().hash();
        let mut new_entry_count = 0xdeadbeaf;
        let element_count = self.element_count;

        let new_root = match self.root.try_borrow_owned() {
            OwnedNode(mutable) => {
                if mutable.try_insert_in_place(hash, 0, kvp.clone(), &mut new_entry_count) {
                    None
                } else {
                    Some(mutable.insert(hash, 0, kvp, &mut new_entry_count))
                }
            }
            SharedNode(immutable) => {
                Some(immutable.insert(hash, 0, kvp, &mut new_entry_count))
            }
        };

        let new_root = match new_root {
            Some(r) => r,
            None => self.root
        };

        // let new_root = self.root.borrow().insert(hash,
        //                                          0,
        //                                          kvp,
        //                                          &mut new_entry_count);
        assert!(new_entry_count != 0xdeadbeaf);

        (
            HamtMap {
                root: new_root,
                element_count: element_count + new_entry_count
            },
            new_entry_count != 0
        )
    }

    fn remove(self, key: &K) -> (HamtMap<K, V, IS>, bool) {
        let hash = key.hash();
        let mut removal_count = 0xdeadbeaf;
        let removal_result = self.root.borrow().remove(hash, 0, key, &mut removal_count);
        assert!(removal_count != 0xdeadbeaf);
        let new_element_count = self.element_count - removal_count;

        (match removal_result {
            NoChange => HamtMap {
                root: self.root.clone(),
                element_count: new_element_count
            },
            ReplaceSubTree(new_root) => HamtMap {
                root: new_root,
                element_count: new_element_count
            },
            CollapseSubTree(kvp) => {
                assert!(bit_count(self.root.borrow().mask) == 2);
                let local_key = (kvp.key().hash() & LEVEL_BIT_MASK) as uint;

                let mask = 1 << local_key;
                let mut root_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
                {
                    let root = root_ref.borrow_mut();
                    root.init_entry(0, SingleItem(kvp));
                }
                HamtMap {
                    root: root_ref,
                    element_count: new_element_count
                }
            }
            KillSubTree => {
                assert!(bit_count(self.root.borrow().mask) == 1);
                HamtMap::new()
            }
        }, removal_count != 0)
    }
}

#[inline]
fn get_index(mask: u32, index: uint) -> uint {
    assert!((mask & (1 << index)) != 0);

    let bits_set_up_to_index = (1 << index) - 1;
    let masked = mask & bits_set_up_to_index;

    bit_count(masked)
}

#[inline]
fn bit_count(x: u32) -> uint {
    unsafe {
        intrinsics::ctpop32(cast::transmute(x)) as uint
    }
}

// Copy --------------------------------------------------------------------------------------------


struct HamtMapCopy<K, V> {
    map: HamtMap<K, V, CopyStore<K, V>>
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> HamtMapCopy<K, V> {
    #[inline]
    pub fn new() -> HamtMapCopy<K, V> {
        HamtMapCopy {
            map: HamtMap::new()
        }
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Clone for HamtMapCopy<K, V> {
    #[inline]
    fn clone(&self) -> HamtMapCopy<K, V> {
        HamtMapCopy { map: self.map.clone() }
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> PersistentMap<K, V> for HamtMapCopy<K, V> {
    fn insert(self, key: K, value: V) -> (HamtMapCopy<K, V>, bool) {
        let (new_map, flag) = self.map.insert(CopyStore { key: key, val: value });
        (HamtMapCopy { map: new_map }, flag)
    }

    fn remove(self, key: &K) -> (HamtMapCopy<K, V>, bool) {
        let (new_map, flag) = self.map.remove(key);
        (HamtMapCopy { map: new_map }, flag)
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Map<K, V> for HamtMapCopy<K, V> {
    #[inline]
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        self.map.find(key)
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Container for HamtMapCopy<K, V> {
    #[inline]
    fn len<'a>(&'a self) -> uint {
        self.map.len()
    }
}

// Share -------------------------------------------------------------------------------------------
struct HamtMapShare<K, V> {
    map: HamtMap<K, V, ShareStore<K, V>>
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> HamtMapShare<K, V> {
    #[inline]
    pub fn new() -> HamtMapShare<K, V> {
        HamtMapShare {
            map: HamtMap::new()
        }
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Clone for HamtMapShare<K, V> {
    #[inline]
    fn clone(&self) -> HamtMapShare<K, V> {
        HamtMapShare { map: self.map.clone() }
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> PersistentMap<K, V> for HamtMapShare<K, V> {
    fn insert(self, key: K, value: V) -> (HamtMapShare<K, V>, bool) {
        let (new_map, flag) = self.map.insert(ShareStore { store: Arc::new((key,value)) });
        (HamtMapShare { map: new_map }, flag)
    }

    fn remove(self, key: &K) -> (HamtMapShare<K, V>, bool) {
        let (new_map, flag) = self.map.remove(key);
        (HamtMapShare { map: new_map }, flag)
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Map<K, V> for HamtMapShare<K, V> {
    #[inline]
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        self.map.find(key)
    }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Container for HamtMapShare<K, V> {
    #[inline]
    fn len<'a>(&'a self) -> uint {
        self.map.len()
    }
}


#[cfg(test)]
mod tests {
    use super::get_index;
    use super::{HamtMapCopy, HamtMapShare};
    use test::Test;
    use extra::test::BenchHarness;

    #[test]
    fn test_get_index() {
        assert_eq!(get_index(0b00000000000000000000000000000001, 0), 0);
        assert_eq!(get_index(0b00000000000000000000000000000010, 1), 0);
        assert_eq!(get_index(0b00000000000000000000000000000100, 2), 0);
        assert_eq!(get_index(0b10000000000000000000000000000000, 31), 0);

        assert_eq!(get_index(0b00000000000000000000000000101010, 1), 0);
        assert_eq!(get_index(0b00000000000000000000000000101010, 3), 1);
        assert_eq!(get_index(0b00000000000000000000000000101010, 5), 2);
    }

    #[test]
    fn test_insert_copy() { Test::test_insert(HamtMapCopy::<u64, u64>::new()); }

    #[test]
    fn test_insert_ascending_copy() { Test::test_insert_ascending(HamtMapCopy::<u64, u64>::new()); }

    #[test]
    fn test_insert_descending_copy() {
        Test::test_insert_descending(HamtMapCopy::<u64, u64>::new());
    }

    #[test]
    fn test_insert_overwrite_copy() { Test::test_insert_overwrite(HamtMapCopy::<u64, u64>::new()); }

    #[test]
    fn test_remove_copy() { Test::test_remove(HamtMapCopy::<u64, u64>::new()); }

    #[test]
    fn test_random_copy() { Test::test_random(HamtMapCopy::<u64, u64>::new()); }

    #[bench]
    fn bench_insert_copy_10(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapCopy::<u64, u64>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_copy_100(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapCopy::<u64, u64>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_copy_1000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapCopy::<u64, u64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_copy_50000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapCopy::<u64, u64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_copy_10(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapCopy::<u64, u64>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_copy_100(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapCopy::<u64, u64>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_copy_1000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapCopy::<u64, u64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_copy_50000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapCopy::<u64, u64>::new(), 50000, bh);
    }


    #[test]
    fn test_insert_share() { Test::test_insert(HamtMapShare::<u64, u64>::new()); }

    #[test]
    fn test_insert_ascending_share() {
        Test::test_insert_ascending(HamtMapShare::<u64, u64>::new());
    }

    #[test]
    fn test_insert_descending_share() {
        Test::test_insert_descending(HamtMapShare::<u64, u64>::new());
    }

    #[test]
    fn test_insert_overwrite_share() { Test::test_insert_overwrite(HamtMapShare::<u64, u64>::new()); }

    #[test]
    fn test_remove_share() { Test::test_remove(HamtMapShare::<u64, u64>::new()); }

    #[test]
    fn test_random_share() { Test::test_random(HamtMapShare::<u64, u64>::new()); }

    #[bench]
    fn bench_insert_share_10(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapShare::<u64, u64>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_share_100(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapShare::<u64, u64>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_share_1000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapShare::<u64, u64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_share_50000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMapShare::<u64, u64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_share_10(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapShare::<u64, u64>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_share_100(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapShare::<u64, u64>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_share_1000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapShare::<u64, u64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_share_50000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMapShare::<u64, u64>::new(), 50000, bh);
    }
}
