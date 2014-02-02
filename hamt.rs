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
use std::ptr;
use std::vec;
use std::unstable::intrinsics;
use std::sync::atomics::{AtomicUint, Acquire, Release};
use extra::arc::Arc;
use persistent::PersistentMap;


static LAST_LEVEL: uint = (64 / 5) - 1;
static BITS_PER_LEVEL: uint = 5;
static LEVEL_BIT_MASK: u64 = 0b11111;

trait ItemStore<K, V> : Clone {
    fn key<'a>(&'a self) -> &'a K;
    fn val<'a>(&'a self) -> &'a V;
}

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

struct Node0<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..0] }
struct Node1<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..1] }
struct Node2<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..2] }
struct Node3<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..3] }
struct Node4<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..4] }
struct Node5<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..5] }
struct Node6<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..6] }
struct Node7<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..7] }
struct Node8<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..8] }
struct Node9<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..9] }
struct Node10<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..10] }
struct Node11<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..11] }
struct Node12<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..12] }
struct Node13<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..13] }
struct Node14<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..14] }
struct Node15<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..15] }
struct Node16<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..16] }
struct Node17<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..17] }
struct Node18<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..18] }
struct Node19<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..19] }
struct Node20<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..20] }
struct Node21<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..21] }
struct Node22<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..22] }
struct Node23<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..23] }
struct Node24<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..24] }
struct Node25<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..25] }
struct Node26<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..26] }
struct Node27<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..27] }
struct Node28<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..28] }
struct Node29<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..29] }
struct Node30<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..30] }
struct Node31<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..31] }
struct Node32<K, V, IS> { ref_count: AtomicUint, mask: u32, entries: [NodeEntry<K, V, IS>, ..32] }

struct UnsafeNode<K, V, IS> {
    ref_count: AtomicUint,
    mask: u32,
    __entries: [NodeEntry<K, V, IS>, ..0],
}

impl<K, V, IS> UnsafeNode<K, V, IS> {
    fn get_entry<'a>(&'a self, index: uint) -> &'a NodeEntry<K, V, IS> {
        unsafe {
            assert!(index < self.entry_count());
            let base: *NodeEntry<K, V, IS> = cast::transmute(&self.__entries);
            let entry_ptr = base.offset(index as int);
            cast::transmute(entry_ptr)
        }
    }

    fn entry_count(&self) -> uint {
        bit_count(self.mask)
    }

    fn alloc(mask: u32) -> NodeRef<K, V, IS> {
        unsafe {
            let node_ptr: *mut UnsafeNode<K, V, IS> = match bit_count(mask) {
                0 => cast::transmute(~intrinsics::uninit::<Node0<K, V, IS>>()),
                1 => cast::transmute(~intrinsics::uninit::<Node1<K, V, IS>>()),
                2 => cast::transmute(~intrinsics::uninit::<Node2<K, V, IS>>()),
                3 => cast::transmute(~intrinsics::uninit::<Node3<K, V, IS>>()),
                4 => cast::transmute(~intrinsics::uninit::<Node4<K, V, IS>>()),
                5 => cast::transmute(~intrinsics::uninit::<Node5<K, V, IS>>()),
                6 => cast::transmute(~intrinsics::uninit::<Node6<K, V, IS>>()),
                7 => cast::transmute(~intrinsics::uninit::<Node7<K, V, IS>>()),
                8 => cast::transmute(~intrinsics::uninit::<Node8<K, V, IS>>()),
                9 => cast::transmute(~intrinsics::uninit::<Node9<K, V, IS>>()),
                10 => cast::transmute(~intrinsics::uninit::<Node10<K, V, IS>>()),
                11 => cast::transmute(~intrinsics::uninit::<Node11<K, V, IS>>()),
                12 => cast::transmute(~intrinsics::uninit::<Node12<K, V, IS>>()),
                13 => cast::transmute(~intrinsics::uninit::<Node13<K, V, IS>>()),
                14 => cast::transmute(~intrinsics::uninit::<Node14<K, V, IS>>()),
                15 => cast::transmute(~intrinsics::uninit::<Node15<K, V, IS>>()),
                16 => cast::transmute(~intrinsics::uninit::<Node16<K, V, IS>>()),
                17 => cast::transmute(~intrinsics::uninit::<Node17<K, V, IS>>()),
                18 => cast::transmute(~intrinsics::uninit::<Node18<K, V, IS>>()),
                19 => cast::transmute(~intrinsics::uninit::<Node19<K, V, IS>>()),
                20 => cast::transmute(~intrinsics::uninit::<Node20<K, V, IS>>()),
                21 => cast::transmute(~intrinsics::uninit::<Node21<K, V, IS>>()),
                22 => cast::transmute(~intrinsics::uninit::<Node22<K, V, IS>>()),
                23 => cast::transmute(~intrinsics::uninit::<Node23<K, V, IS>>()),
                24 => cast::transmute(~intrinsics::uninit::<Node24<K, V, IS>>()),
                25 => cast::transmute(~intrinsics::uninit::<Node25<K, V, IS>>()),
                26 => cast::transmute(~intrinsics::uninit::<Node26<K, V, IS>>()),
                27 => cast::transmute(~intrinsics::uninit::<Node27<K, V, IS>>()),
                28 => cast::transmute(~intrinsics::uninit::<Node28<K, V, IS>>()),
                29 => cast::transmute(~intrinsics::uninit::<Node29<K, V, IS>>()),
                30 => cast::transmute(~intrinsics::uninit::<Node30<K, V, IS>>()),
                31 => cast::transmute(~intrinsics::uninit::<Node31<K, V, IS>>()),
                32 => cast::transmute(~intrinsics::uninit::<Node32<K, V, IS>>()),
                _ => fail!("")
            };

            intrinsics::move_val_init(&mut (*node_ptr).ref_count, AtomicUint::new(1));
            intrinsics::move_val_init(&mut (*node_ptr).mask, mask);

            NodeRef { ptr: node_ptr }
        }
    }

    fn destroy(&self) {
        unsafe {
            let ptr: *UnsafeNode<K, V, IS> = cast::transmute(self);
            match self.entry_count() {
                0 => { let _: ~Node0<K, V, IS> = cast::transmute(ptr); }
                1 => { let _: ~Node1<K, V, IS> = cast::transmute(ptr); }
                2 => { let _: ~Node2<K, V, IS> = cast::transmute(ptr); }
                3 => { let _: ~Node3<K, V, IS> = cast::transmute(ptr); }
                4 => { let _: ~Node4<K, V, IS> = cast::transmute(ptr); }
                5 => { let _: ~Node5<K, V, IS> = cast::transmute(ptr); }
                6 => { let _: ~Node6<K, V, IS> = cast::transmute(ptr); }
                7 => { let _: ~Node7<K, V, IS> = cast::transmute(ptr); }
                8 => { let _: ~Node8<K, V, IS> = cast::transmute(ptr); }
                9 => { let _: ~Node9<K, V, IS> = cast::transmute(ptr); }
                10 => { let _: ~Node10<K, V, IS> = cast::transmute(ptr); }
                11 => { let _: ~Node11<K, V, IS> = cast::transmute(ptr); }
                12 => { let _: ~Node12<K, V, IS> = cast::transmute(ptr); }
                13 => { let _: ~Node13<K, V, IS> = cast::transmute(ptr); }
                14 => { let _: ~Node14<K, V, IS> = cast::transmute(ptr); }
                15 => { let _: ~Node15<K, V, IS> = cast::transmute(ptr); }
                16 => { let _: ~Node16<K, V, IS> = cast::transmute(ptr); }
                17 => { let _: ~Node17<K, V, IS> = cast::transmute(ptr); }
                18 => { let _: ~Node18<K, V, IS> = cast::transmute(ptr); }
                19 => { let _: ~Node19<K, V, IS> = cast::transmute(ptr); }
                20 => { let _: ~Node20<K, V, IS> = cast::transmute(ptr); }
                21 => { let _: ~Node21<K, V, IS> = cast::transmute(ptr); }
                22 => { let _: ~Node22<K, V, IS> = cast::transmute(ptr); }
                23 => { let _: ~Node23<K, V, IS> = cast::transmute(ptr); }
                24 => { let _: ~Node24<K, V, IS> = cast::transmute(ptr); }
                25 => { let _: ~Node25<K, V, IS> = cast::transmute(ptr); }
                26 => { let _: ~Node26<K, V, IS> = cast::transmute(ptr); }
                27 => { let _: ~Node27<K, V, IS> = cast::transmute(ptr); }
                28 => { let _: ~Node28<K, V, IS> = cast::transmute(ptr); }
                29 => { let _: ~Node29<K, V, IS> = cast::transmute(ptr); }
                30 => { let _: ~Node30<K, V, IS> = cast::transmute(ptr); }
                31 => { let _: ~Node31<K, V, IS> = cast::transmute(ptr); }
                32 => { let _: ~Node32<K, V, IS> = cast::transmute(ptr); }
                _ => fail!("")
            }
        }
    }
}

struct NodeRef<K, V, IS> {
    ptr: *mut UnsafeNode<K, V, IS>
}

impl<K, V, IS> NodeRef<K, V, IS> {
    fn borrow<'a>(&'a self) -> &'a UnsafeNode<K, V, IS> {
        unsafe {
            cast::transmute(self.ptr)
        }
    }

    fn set_entry<'a>(&mut self, index: uint, entry: NodeEntry<K, V, IS>) {
        unsafe {
            assert!(index < (*self.ptr).entry_count());
            let base: *mut NodeEntry<K, V, IS> = cast::transmute(&(*self.ptr).__entries);
            let entry_ptr: &mut NodeEntry<K, V, IS> = cast::transmute(base.offset(index as int));
            intrinsics::move_val_init( entry_ptr, entry);
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

impl<K:Hash + Eq + Send + Freeze, V: Send + Freeze, IS: ItemStore<K, V> + Send + Freeze> UnsafeNode<K, V, IS> {
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
        let mut new_node = UnsafeNode::alloc(new_mask);

        let index = get_index(new_mask, local_key);

        let mut old_i = 0;
        let mut new_i = 0;

        // Copy up to index
        while old_i < index {
            new_node.set_entry(new_i, self.get_entry(old_i).clone());
            old_i += 1;
            new_i += 1;
        }

        // Add new entry
        new_node.set_entry(new_i, new_entry);
        new_i += 1;

        if replace_old_entry {
            // Skip the replaced value
            old_i += 1;
        }

        // Copy the rest
        while old_i < self.entry_count() {
            new_node.set_entry(new_i, self.get_entry(old_i).clone());
            old_i += 1;
            new_i += 1;
        }

        assert!(new_i == bit_count(new_mask));

        return new_node;
    }

    fn copy_without_entry(&self, local_key: uint) -> NodeRef<K, V, IS> {
        assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let mut new_node = UnsafeNode::alloc(new_mask);
        let index = get_index(self.mask, local_key);

        let mut old_i = 0;
        let mut new_i = 0;

        // Copy up to index
        while old_i < index {
            new_node.set_entry(new_i, self.get_entry(old_i).clone());
            old_i += 1;
            new_i += 1;
        }

        old_i += 1;

        // Copy the rest
        while old_i < self.entry_count() {
            new_node.set_entry(new_i, self.get_entry(old_i).clone());
            old_i += 1;
            new_i += 1;
        }

        assert!(new_i == bit_count(new_mask));

        return new_node;
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
            let mut new_node = UnsafeNode::alloc(mask);
            if new_local_key < existing_local_key {
                new_node.set_entry(0, SingleItem(new_kvp));
                new_node.set_entry(1, SingleItem(existing_kvp.clone()));
            } else {
                new_node.set_entry(0, SingleItem(existing_kvp.clone()));
                new_node.set_entry(1, SingleItem(new_kvp));
            };

            new_node
        } else if level == LAST_LEVEL {
            let mask = 1 << new_local_key;
            let mut new_node = UnsafeNode::alloc(mask);
            new_node.set_entry(0, Collision(Arc::new(~[new_kvp, existing_kvp.clone()])));
            new_node
        } else {
            // recurse further
            let sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                        new_hash >> BITS_PER_LEVEL,
                                                        existing_kvp,
                                                        existing_hash >> BITS_PER_LEVEL,
                                                        level + 1);
            let mask = 1 << new_local_key;
            let mut new_node = UnsafeNode::alloc(mask);
            new_node.set_entry(0, SubTree(sub_tree));
            new_node
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
            root: UnsafeNode::alloc(0),
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

    fn insert(self, kvp: IS) -> (HamtMap<K, V, IS>, bool) {
        let hash = kvp.key().hash();
        let mut new_entry_count = 0xdeadbeaf;
        let new_root = self.root.borrow().insert(hash,
                                                 0,
                                                 kvp,
                                                 &mut new_entry_count);
        assert!(new_entry_count != 0xdeadbeaf);

        (
            HamtMap {
                root: new_root,
                element_count: self.element_count + new_entry_count
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
                let mut root = UnsafeNode::alloc(mask);
                root.set_entry(0, SingleItem(kvp));

                HamtMap {
                    root: root,
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
struct CopyStore<K, V> {
    key: K,
    val: V
}

impl<K: Clone, V: Clone> ItemStore<K, V> for CopyStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { &self.key }
    fn val<'a>(&'a self) -> &'a V { &self.val }
}

impl<K: Clone, V: Clone> Clone for CopyStore<K, V> {
    fn clone(&self) -> CopyStore<K, V> {
        CopyStore {
            key: self.key.clone(),
            val: self.val.clone(),
        }
    }
}

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
struct ShareStore<K, V> {
    store: Arc<(K, V)>,
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> ItemStore<K, V> for ShareStore<K, V> {
    fn key<'a>(&'a self) -> &'a K { self.store.get().first_ref() }
    fn val<'a>(&'a self) -> &'a V { self.store.get().second_ref() }
}

impl<K: Hash+Eq+Send+Freeze+Clone, V: Send+Freeze+Clone> Clone for ShareStore<K, V> {
    fn clone(&self) -> ShareStore<K, V> {
        ShareStore {
            store: self.store.clone()
        }
    }
}

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
