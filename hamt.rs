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

// Hash Array Mapped Trie Implementation
// Based on "Ideal Hash Trees" by Phil Bagwell:
// http://lampwww.epfl.ch/papers/idealhashtrees.pdf

use std::cast;
use std::mem;
use std::ptr;
use std::vec;
use std::unstable::intrinsics;
use std::sync::atomics::{AtomicUint, Acquire, Release};
use std::rt::global_heap::{exchange_malloc, exchange_free};

use sync::Arc;
use persistent::PersistentMap;
use item_store::{ItemStore, CopyStore, ShareStore};


//=-------------------------------------------------------------------------------------------------
// NodeRef
//=-------------------------------------------------------------------------------------------------
struct NodeRef<K, V, IS> {
    ptr: *mut UnsafeNode<K, V, IS>
}

enum NodeRefBorrowResult<'a, K, V, IS> {
    OwnedNode(&'a mut UnsafeNode<K, V, IS>),
    SharedNode(&'a UnsafeNode<K, V, IS>),
}

impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V>> NodeRef<K, V, IS> {
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



//=-------------------------------------------------------------------------------------------------
// UnsafeNode
//=-------------------------------------------------------------------------------------------------
static BITS_PER_LEVEL: uint = 5;
static LAST_LEVEL: uint = (64 / BITS_PER_LEVEL) - 1;
static LEVEL_BIT_MASK: u64 = (1 << BITS_PER_LEVEL) - 1;
static MIN_CAPACITY: uint = 4;

struct AlignmentStruct<K, V, IS> {
    a: Arc<~[IS]>,
    b: IS,
    c: NodeRef<K, V, IS>
}

struct UnsafeNode<K, V, IS> {
    ref_count: AtomicUint,
    entry_types: u64,
    mask: u32,
    capacity: u8,
    __entries: [AlignmentStruct<K, V, IS>, ..0],
}

enum NodeEntry<'a, K, V, IS> {
    Collision(&'a Arc<~[IS]>),
    SingleItem(&'a IS),
    SubTree(&'a NodeRef<K, V, IS>)
}

impl<'a, K: Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V>> NodeEntry<'a, K, V, IS> {
    fn clone_out(&self) -> NodeEntryOwned<K, V, IS> {
        match *self {
            Collision(r) => CollisionOwned(r.clone()),
            SingleItem(is) => SingleItemOwned(is.clone()),
            SubTree(r) => SubTreeOwned(r.clone()),
        }
    }
}

static KVP_ENTRY: uint = 0b01;
static SUBTREE_ENTRY: uint = 0b10;
static COLLISION_ENTRY: uint = 0b11;

enum NodeEntryMut<'a, K, V, IS> {
    CollisionMut(&'a mut Arc<~[IS]>),
    SingleItemMut(&'a mut IS),
    SubTreeMut(&'a mut NodeRef<K, V, IS>)
}

enum NodeEntryOwned<K, V, IS> {
    CollisionOwned(Arc<~[IS]>),
    SingleItemOwned(IS),
    SubTreeOwned(NodeRef<K, V, IS>)
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

// impl UnsafeNode
impl<K, V, IS> UnsafeNode<K, V, IS> {

    fn get_entry_type_code(&self, index: uint) -> uint {
        ((self.entry_types >> (index * 2)) & 0b11) as uint
    }

    fn set_entry_type_code(&mut self, index: uint, type_code: uint) {
        assert!(type_code <= 0b11);
        self.entry_types = (self.entry_types & !(0b11 << (index * 2))) |
                           (type_code as u64 << (index * 2));
    }

    fn get_entry_ptr(&self, index: uint) -> *u8 {
        assert!(index < self.entry_count());
        unsafe {
            let base: *u8 = cast::transmute(&self.__entries);
            base.offset((index * UnsafeNode::<K, V, IS>::node_entry_size()) as int)
        }
    }

    fn get_entry<'a>(&'a self, index: uint) -> NodeEntry<'a, K, V, IS> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => SingleItem(cast::transmute(entry_ptr)),
                SUBTREE_ENTRY => SubTree(cast::transmute(entry_ptr)),
                COLLISION_ENTRY => Collision(cast::transmute(entry_ptr)),
                _ => fail!("Invalid entry type code")
            }
        }
    }

    fn get_entry_mut<'a>(&'a mut self, index: uint) -> NodeEntryMut<'a, K, V, IS> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => SingleItemMut(cast::transmute(entry_ptr)),
                SUBTREE_ENTRY => SubTreeMut(cast::transmute(entry_ptr)),
                COLLISION_ENTRY => CollisionMut(cast::transmute(entry_ptr)),
                _ => fail!("Invalid entry type code")
            }
        }
    }

    fn init_entry<'a>(&mut self, index: uint, entry: NodeEntryOwned<K, V, IS>) {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match entry {
                SingleItemOwned(kvp) => {
                    intrinsics::move_val_init(cast::transmute(entry_ptr), kvp);
                    self.set_entry_type_code(index, KVP_ENTRY);
                }
                SubTreeOwned(node_ref) => {
                    intrinsics::move_val_init(cast::transmute(entry_ptr), node_ref);
                    self.set_entry_type_code(index, SUBTREE_ENTRY);
                }
                CollisionOwned(arc) => {
                    intrinsics::move_val_init(cast::transmute(entry_ptr), arc);
                    self.set_entry_type_code(index, COLLISION_ENTRY);
                }
            }
        }
    }

    fn entry_count(&self) -> uint {
        bit_count(self.mask)
    }

    fn node_entry_size() -> uint {
        ::std::num::max(
            mem::size_of::<IS>(),
            ::std::num::max(
                mem::size_of::<Arc<~[IS]>>(),
                mem::size_of::<NodeRef<K, V, IS>>(),
            )
        )
    }

    fn alloc(mask: u32, capacity: uint) -> NodeRef<K, V, IS> {
        assert!(size_of_zero_entry_array::<K, V, IS>() == 0);
        fn size_of_zero_entry_array<K, V, IS>() -> uint {
            let node: UnsafeNode<K, V, IS> = UnsafeNode {
                ref_count: AtomicUint::new(0),
                entry_types: 0,
                mask: 0,
                capacity: 0,
                __entries: [],
            };

            mem::size_of_val(&node.__entries)
        }

        fn align_to(size: uint, align: uint) -> uint {
            assert!(align != 0 && bit_count(align as u32) == 1);
            (size + align - 1) & !(align - 1)
        }

        let align = mem::pref_align_of::<AlignmentStruct<K, V, IS>>();
        let entry_count = bit_count(mask);
        assert!(entry_count <= capacity);

        let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS>>(), align);
        let node_size = header_size + capacity * UnsafeNode::<K, V, IS>::node_entry_size();

        unsafe {
            let node_ptr: *mut UnsafeNode<K, V, IS> = cast::transmute(exchange_malloc(node_size));
            intrinsics::move_val_init(&mut (*node_ptr).ref_count, AtomicUint::new(1));
            intrinsics::move_val_init(&mut (*node_ptr).entry_types, 0);
            intrinsics::move_val_init(&mut (*node_ptr).mask, mask);
            intrinsics::move_val_init(&mut (*node_ptr).capacity, capacity as u8);
            NodeRef { ptr: node_ptr }
        }
    }

    fn destroy(&mut self) {
        unsafe {
            let entry_count = self.entry_count();

            for i in range(0, entry_count) {
                self.drop_entry(i)
            }

            exchange_free(cast::transmute(self));
        }
    }

    unsafe fn drop_entry(&mut self, index: uint) {
        // destroy the contained object, trick from Rc
        match self.get_entry_mut(index) {
            SingleItemMut(item_ref) => {
                let _ = ptr::read_ptr(ptr::to_unsafe_ptr(item_ref));
            }
            CollisionMut(item_ref) => {
                let _ = ptr::read_ptr(ptr::to_unsafe_ptr(item_ref));
            }
            SubTreeMut(item_ref) => {
                let _ = ptr::read_ptr(ptr::to_unsafe_ptr(item_ref));
            }
        }
    }
}

// impl UnsafeNode (continued)
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze, IS: ItemStore<K, V>> UnsafeNode<K, V, IS> {
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
            let new_node = self.copy_with_new_entry(local_key, SingleItemOwned(new_kvp));
            return new_node;
        }

        let index = get_index(self.mask, local_key);

        match self.get_entry(index) {
            SingleItem(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    self.copy_with_new_entry(local_key, SingleItemOwned(new_kvp))
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
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    self.copy_with_new_entry(local_key, SubTreeOwned(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = ~[new_kvp, existing_kvp_ref.clone()];
                    self.copy_with_new_entry(local_key, CollisionOwned(Arc::new(items)))
                }
            }
            Collision(items_ref) => {
                assert!(level == LAST_LEVEL);
                let items = items_ref.get();
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

                self.copy_with_new_entry(local_key, CollisionOwned(Arc::new(new_items)))
            }
            SubTree(sub_tree_ref) => {
                let new_sub_tree = sub_tree_ref.borrow().insert(hash >> BITS_PER_LEVEL,
                                                                level + 1,
                                                                new_kvp,
                                                                insertion_count);

                self.copy_with_new_entry(local_key, SubTreeOwned(new_sub_tree))
            }
        }
    }

    fn try_insert_in_place(&mut self,
                           hash: u64,
                           level: uint,
                           new_kvp: IS,
                           insertion_count: &mut uint)
                        -> Option<NodeRef<K, V, IS>> {
        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if !self.can_insert_in_place(local_key) {
            return Some(self.insert(hash, level, new_kvp, insertion_count));
        }

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            self.insert_entry_in_place(local_key, SingleItemOwned(new_kvp));
            return None;
        }

        let index = get_index(self.mask, local_key);

        let new_entry = match self.get_entry_mut(index) {
            SingleItemMut(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    Some(SingleItemOwned(new_kvp))
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
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    Some(SubTreeOwned(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = ~[new_kvp, existing_kvp_ref.clone()];
                    let collision_entry = CollisionOwned(Arc::new(items));
                    Some(collision_entry)
                }
            }
            CollisionMut(items_ref) => {
                assert!(level == LAST_LEVEL);
                let items = items_ref.get();
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

                Some(CollisionOwned(Arc::new(new_items)))
            }
            SubTreeMut(subtree_mut_ref) => {
                match subtree_mut_ref.try_borrow_owned() {
                    SharedNode(subtree) => {
                        Some(SubTreeOwned(subtree.insert(hash >> BITS_PER_LEVEL,
                                                         level + 1,
                                                         new_kvp,
                                                         insertion_count)))
                    }
                    OwnedNode(subtree) => {
                        match subtree.try_insert_in_place(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          new_kvp.clone(),
                                                          insertion_count) {
                            Some(new_sub_tree) => Some(SubTreeOwned(new_sub_tree)),
                            None => None
                        }
                    }
                }
            }
        };

        match new_entry {
            Some(e) => {
                self.insert_entry_in_place(local_key, e);
            }
            None => {
                /* No new entry to be inserted, because the subtree could be modified in-place */
            }
        }

        return None;
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

        match self.get_entry(index) {
            SingleItem(existing_kvp_ref) => {
                if *existing_kvp_ref.key() == *key {
                    *removal_count = 1;
                    self.collapse_kill_or_change(local_key, index)
                } else {
                    *removal_count = 0;
                    NoChange
                }
            }
            Collision(items_ref) => {
                assert!(level == LAST_LEVEL);
                let items = items_ref.get();
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

                            CollisionOwned(Arc::new(new_items))
                        } else {
                            assert!(items.len() == 2);
                            assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = items[index_of_remaining_item].clone();

                            SingleItemOwned(kvp)
                        };

                        let new_sub_tree = self.copy_with_new_entry(local_key, new_entry);
                        ReplaceSubTree(new_sub_tree)
                    }
                }
            }
            SubTree(sub_tree_ref) => {
                let result = sub_tree_ref.borrow().remove(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          key,
                                                          removal_count);
                match result {
                    NoChange => NoChange,
                    ReplaceSubTree(x) => {
                        ReplaceSubTree(self.copy_with_new_entry(local_key, SubTreeOwned(x)))
                    }
                    CollapseSubTree(kvp) => {
                        if bit_count(self.mask) == 1 {
                            CollapseSubTree(kvp)
                        } else {
                            ReplaceSubTree(self.copy_with_new_entry(local_key, SingleItemOwned(kvp)))
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
        let new_entry_count = bit_count(self.mask) - 1;

        if new_entry_count > 1 {
            ReplaceSubTree(self.copy_without_entry(local_key))
        } else if new_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.get_entry(other_index) {
                SingleItem(kvp_ref) => {
                    CollapseSubTree(kvp_ref.clone())
                }
                _ => ReplaceSubTree(self.copy_without_entry(local_key))
            }
        } else {
            assert!(new_entry_count == 0);
            KillSubTree
        }
    }

    fn copy_with_new_entry(&self,
                           local_key: uint,
                           new_entry: NodeEntryOwned<K, V, IS>)
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
                new_node.init_entry(new_i, self.get_entry(old_i).clone_out());
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
                new_node.init_entry(new_i, self.get_entry(old_i).clone_out());
                old_i += 1;
                new_i += 1;
            }

            assert!(new_i == new_node.entry_count() as uint);
        }

        return new_node_ref;
    }

    fn can_insert_in_place(&self, local_key: uint) -> bool {
        let bit = (1 << local_key);
        if (self.mask & bit) != 0 {
            true
        } else {
            self.entry_count() < self.capacity as uint
        }
    }

    fn insert_entry_in_place(&mut self,
                             local_key: uint,
                             new_entry: NodeEntryOwned<K, V, IS>) {
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
                    let source: *u8 = self.get_entry_ptr(index);
                    let dest: *mut u8 = cast::transmute_mut_unsafe(
                        source.offset(UnsafeNode::<K, V, IS>::node_entry_size() as int));
                    let count = (self.entry_count() - index) *
                        UnsafeNode::<K, V, IS>::node_entry_size();
                    ptr::copy_memory(dest, source, count);

                    let type_mask_up_to_index: u64 = 0xFFFFFFFFFFFFFFFF << ((index + 1) * 2);
                    self.entry_types = ((self.entry_types << 2) & type_mask_up_to_index) |
                                       (self.entry_types & !type_mask_up_to_index);
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
                new_node.init_entry(new_i, self.get_entry(old_i).clone_out());
                old_i += 1;
                new_i += 1;
            }

            old_i += 1;

            // Copy the rest
            while old_i < self.entry_count() {
                new_node.init_entry(new_i, self.get_entry(old_i).clone_out());
                old_i += 1;
                new_i += 1;
            }

            assert!(new_i == bit_count(new_mask));
        }
        return new_node_ref;
    }

    // fn remove_entry_in_place(&mut self, local_key: uint) {
     // assert!((self.mask & (1 << local_key)) != 0);

    //     let new_mask = self.mask & !(1 << local_key);
    //     let index = get_index(self.mask, local_key);

    //     unsafe {
    //         self.drop_entry(index);

    //         let source = ptr::to_unsafe_ptr(self.get_entry(index + 1));
    //         let dest = cast::transmute_mut_unsafe(source.offset(-1));
    //         let count = self.entry_count() - (index + 1);
    //         ptr::copy_memory(dest, source, count);
    //     }

    //     self.mask = new_mask;
    // }

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
                    new_node.init_entry(0, SingleItemOwned(new_kvp));
                    new_node.init_entry(1, SingleItemOwned(existing_kvp.clone()));
                } else {
                    new_node.init_entry(0, SingleItemOwned(existing_kvp.clone()));
                    new_node.init_entry(1, SingleItemOwned(new_kvp));
                };
            }
            new_node_ref
        } else if level == LAST_LEVEL {
            let mask = 1 << new_local_key;
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();
                new_node.init_entry(0, CollisionOwned(Arc::new(~[new_kvp, existing_kvp.clone()])));
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
                new_node.init_entry(0, SubTreeOwned(sub_tree));
            }
            new_node_ref
        }
    }
}



//=-------------------------------------------------------------------------------------------------
// HamtMap
//=-------------------------------------------------------------------------------------------------
struct HamtMap<K, V, IS> {
    root: NodeRef<K, V, IS>,
    element_count: uint,
}

// impl HamtMap
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
        let mut current_node = self.root.borrow();

        loop {
            assert!(level <= LAST_LEVEL);
            let local_key = (hash & LEVEL_BIT_MASK) as uint;

            if (current_node.mask & (1 << local_key)) == 0 {
                return None;
            }

            let index = get_index(current_node.mask, local_key);

            match current_node.get_entry(index) {
                SingleItem(kvp_ref) => return if *key == *kvp_ref.key() {
                    Some(kvp_ref.val())
                } else {
                    None
                },
                Collision(items_ref) => {
                    assert!(level == LAST_LEVEL);
                    let found = items_ref.get().iter().find(|&kvp| *key == *kvp.key());
                    return match found {
                        Some(kvp) => Some(kvp.val()),
                        None => None,
                    };
                }
                SubTree(subtree_ref) => {
                    assert!(level < LAST_LEVEL);
                    current_node = subtree_ref.borrow();
                    hash = hash >> BITS_PER_LEVEL;
                    level += 1;
                }
            };
        }
    }

    fn insert_internal(mut self, kvp: IS) -> (HamtMap<K, V, IS>, bool) {
        let hash = kvp.key().hash();
        let mut insertion_count = 0xdeadbeaf;
        let element_count = self.element_count;

        // If we hold the only reference to the root node, then try to insert the KVP in-place
        let new_root = match self.root.try_borrow_owned() {
            OwnedNode(mutable) => mutable.try_insert_in_place(hash, 0, kvp, &mut insertion_count),
            SharedNode(immutable) => Some(immutable.insert(hash, 0, kvp, &mut insertion_count))
        };

        let new_root = match new_root {
            Some(r) => r,
            None => self.root
        };

        // Make sure that insertion_count was set properly
        assert!(insertion_count != 0xdeadbeaf);

        (
            HamtMap {
                root: new_root,
                element_count: element_count + insertion_count
            },
            insertion_count != 0
        )
    }

    fn remove_internal(self, key: &K) -> (HamtMap<K, V, IS>, bool) {
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
                    root.init_entry(0, SingleItemOwned(kvp));
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

// Clone for HamtMap
impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone, IS: ItemStore<K, V>>
Clone for HamtMap<K, V, IS> {

    fn clone(&self) -> HamtMap<K, V, IS> {
        HamtMap { root: self.root.clone(), element_count: self.element_count }
    }
}

// Container for HamtMap
impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone, IS: ItemStore<K, V>>
Container for HamtMap<K, V, IS> {

    fn len(&self) -> uint {
        self.element_count
    }
}

// Map for HamtMap
impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone, IS: ItemStore<K, V>>
Map<K, V> for HamtMap<K, V, IS> {

    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        self.find(key)
    }
}

// PersistentMap for HamtMap<CopyStore>
impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone>
PersistentMap<K, V> for HamtMap<K, V, CopyStore<K, V>> {

    fn insert(self, key: K, value: V) -> (HamtMap<K, V, CopyStore<K, V>>, bool) {
        self.insert_internal(CopyStore { key: key, val: value })
    }

    fn remove(self, key: &K) -> (HamtMap<K, V, CopyStore<K, V>>, bool) {
        self.remove_internal(key)
    }
}

// PersistentMap for HamtMap<ShareStore>
impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone>
PersistentMap<K, V> for HamtMap<K, V, ShareStore<K, V>> {

    fn insert(self, key: K, value: V) -> (HamtMap<K, V, ShareStore<K, V>>, bool) {
        self.insert_internal(ShareStore::new(key,value))
    }

    fn remove(self, key: &K) -> (HamtMap<K, V, ShareStore<K, V>>, bool) {
        self.remove_internal(key)
    }
}

//=-------------------------------------------------------------------------------------------------
// Utility functions
//=------------------------------------------------------------------------------------------------
fn get_index(mask: u32, index: uint) -> uint {
    assert!((mask & (1 << index)) != 0);

    let bits_set_up_to_index = (1 << index) - 1;
    let masked = mask & bits_set_up_to_index;

    bit_count(masked)
}
fn bit_count(x: u32) -> uint {
    unsafe {
        intrinsics::ctpop32(cast::transmute(x)) as uint
    }
}

#[cfg(test)]
mod tests {
    use super::get_index;
    use super::HamtMap;
    use item_store::{CopyStore, ShareStore};
    use test::Test;
    use extra::test::BenchHarness;

    type CopyStoreU64 = CopyStore<uint, uint>;
    type ShareStoreU64 = ShareStore<uint, uint>;

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
    fn test_insert_copy() { Test::test_insert(HamtMap::<uint, uint, CopyStoreU64>::new()); }

    #[test]
    fn test_insert_ascending_copy() { Test::test_insert_ascending(HamtMap::<uint, uint, CopyStoreU64>::new()); }

    #[test]
    fn test_insert_descending_copy() {
        Test::test_insert_descending(HamtMap::<uint, uint, CopyStoreU64>::new());
    }

    #[test]
    fn test_insert_overwrite_copy() { Test::test_insert_overwrite(HamtMap::<uint, uint, CopyStoreU64>::new()); }

    #[test]
    fn test_remove_copy() { Test::test_remove(HamtMap::<uint, uint, CopyStoreU64>::new()); }

    #[test]
    fn test_random_copy() { Test::test_random(HamtMap::<uint, uint, CopyStoreU64>::new()); }

    #[bench]
    fn bench_insert_copy_10(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, CopyStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_copy_100(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, CopyStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_copy_1000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, CopyStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_copy_50000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, CopyStoreU64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_copy_10(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, CopyStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_copy_100(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, CopyStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_copy_1000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, CopyStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_copy_50000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, CopyStoreU64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_remove_copy_10(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, CopyStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_remove_copy_100(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, CopyStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_remove_copy_1000(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, CopyStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_remove_copy_50000(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, CopyStoreU64>::new(), 50000, bh);
    }


//= Shared -----------------------------------------------------------------------------------------


    #[test]
    fn test_insert_share() { Test::test_insert(HamtMap::<uint, uint, ShareStoreU64>::new()); }

    #[test]
    fn test_insert_ascending_share() {
        Test::test_insert_ascending(HamtMap::<uint, uint, ShareStoreU64>::new());
    }

    #[test]
    fn test_insert_descending_share() {
        Test::test_insert_descending(HamtMap::<uint, uint, ShareStoreU64>::new());
    }

    #[test]
    fn test_insert_overwrite_share() { Test::test_insert_overwrite(HamtMap::<uint, uint, ShareStoreU64>::new()); }

    #[test]
    fn test_remove_share() { Test::test_remove(HamtMap::<uint, uint, ShareStoreU64>::new()); }

    #[test]
    fn test_random_share() { Test::test_random(HamtMap::<uint, uint, ShareStoreU64>::new()); }

    #[bench]
    fn bench_insert_share_10(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, ShareStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_share_100(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, ShareStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_share_1000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, ShareStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_share_50000(bh: &mut BenchHarness) {
        Test::bench_insert(HamtMap::<uint, uint, ShareStoreU64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_share_10(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, ShareStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_share_100(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, ShareStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_share_1000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, ShareStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_share_50000(bh: &mut BenchHarness) {
        Test::bench_find(HamtMap::<uint, uint, ShareStoreU64>::new(), 50000, bh);
    }

    #[bench]
    fn bench_remove_share_10(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, ShareStoreU64>::new(), 10, bh);
    }

    #[bench]
    fn bench_remove_share_100(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, ShareStoreU64>::new(), 100, bh);
    }

    #[bench]
    fn bench_remove_share_1000(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, ShareStoreU64>::new(), 1000, bh);
    }

    #[bench]
    fn bench_remove_share_50000(bh: &mut BenchHarness) {
        Test::bench_remove(HamtMap::<uint, uint, ShareStoreU64>::new(), 50000, bh);
    }
}
