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

//! A Hash Array Mapped Trie implementation based on the
//! [Ideal Hash Trees](http://lampwww.epfl.ch/papers/idealhashtrees.pdf) paper by Phil Bagwell.
//! This is the datastructure used by Scala's and Clojure's standard library as map implementation.
//! The idea to use a special *collision node* to deal with hash collisions is taken from Clojure's
//! implementation.

use std::hash::{Hasher, Hash};
use std::intrinsics;
use std::mem;
use std::ptr;
use std::sync::atomics::{AtomicUint, Acquire, Release};
use std::rt::heap;

use sync::Arc;
use PersistentMap;
use item_store::{ItemStore, CopyStore, ShareStore};


//=-------------------------------------------------------------------------------------------------
// NodeRef
//=-------------------------------------------------------------------------------------------------
// A smart pointer dealing for handling node lifetimes, very similar to sync::Arc.
struct NodeRef<K, V, IS, H> {
    ptr: *mut UnsafeNode<K, V, IS, H>
}

// NodeRef knows if it is the only reference to a given node and can thus safely decide to allow for
// mutable access to the referenced node. This type indicates whether mutable access could be
// acquired.
enum NodeRefBorrowResult<'a, K, V, IS, H> {
    OwnedNode(&'a mut UnsafeNode<K, V, IS, H>),
    SharedNode(&'a UnsafeNode<K, V, IS, H>),
}

impl<K: Eq+Send+Share, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>> NodeRef<K, V, IS, H> {

    fn borrow<'a>(&'a self) -> &'a UnsafeNode<K, V, IS, H> {
        unsafe {
            mem::transmute(self.ptr)
        }
    }

    fn borrow_mut<'a>(&'a mut self) -> &'a mut UnsafeNode<K, V, IS, H> {
        unsafe {
            assert!((*self.ptr).ref_count.load(Acquire) == 1);
            mem::transmute(self.ptr)
        }
    }

    // Try to safely gain mutable access to the referenced node. This can be used to safely make
    // in-place modifications instead of unnecessarily copying data.
    fn try_borrow_owned<'a>(&'a mut self) -> NodeRefBorrowResult<'a, K, V, IS, H> {
        unsafe {
            if (*self.ptr).ref_count.load(Acquire) == 1 {
                OwnedNode(mem::transmute(self.ptr))
            } else {
                SharedNode(mem::transmute(self.ptr))
            }
        }
    }
}

#[unsafe_destructor]
impl<K, V, IS, H> Drop for NodeRef<K, V, IS, H> {
    fn drop(&mut self) {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS, H> = mem::transmute(self.ptr);
            let old_count = node.ref_count.fetch_sub(1, Acquire);
            assert!(old_count >= 1);
            if old_count == 1 {
                node.destroy()
            }
        }
    }
}

impl<K, V, IS, H> Clone for NodeRef<K, V, IS, H> {
    fn clone(&self) -> NodeRef<K, V, IS, H> {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS, H> = mem::transmute(self.ptr);
            let old_count = node.ref_count.fetch_add(1, Release);
            assert!(old_count >= 1);
        }

        NodeRef { ptr: self.ptr }
    }
}



//=-------------------------------------------------------------------------------------------------
// UnsafeNode
//=-------------------------------------------------------------------------------------------------
// The number of hash-value bits used per tree-level.
static BITS_PER_LEVEL: uint = 5;
// The deepest level the tree can have. Collision-nodes are use at this depth to avoid any further
// recursion.
static LAST_LEVEL: uint = (64 / BITS_PER_LEVEL) - 1;
// Used to mask off any unused bits from the hash key at a given level.
static LEVEL_BIT_MASK: u64 = (1 << BITS_PER_LEVEL) - 1;
// The minimum node capacity.
static MIN_CAPACITY: uint = 4;

// This struct should have the correct alignment for node entries.
struct AlignmentStruct<K, V, IS, H> {
    a: Arc<Vec<IS>>,
    b: IS,
    c: NodeRef<K, V, IS, H>
}

// Bit signature of node entry types. Every node contains a single u64 designating the kinds of all
// its entries, which can either be a key-value pair, a reference to a sub-tree, or a
// collision-entry, containing a linear list of colliding key-value pairs.
static KVP_ENTRY: uint = 0b01;
static SUBTREE_ENTRY: uint = 0b10;
static COLLISION_ENTRY: uint = 0b11;
static INVALID_ENTRY: uint = 0b00;

// The central node type used by the implementation. Note that this struct just represents the
// header of the node data. The actual entries are allocated directly after this header, starting
// at the address of the `__entries` field.
struct UnsafeNode<K, V, IS, H> {
    // The current number of references to this node.
    ref_count: AtomicUint,
    // The entry types of the of this node. Each two bits encode the type of one entry
    // (key-value pair, subtree ref, or collision entry). See get_entry_type_code() and the above
    // constants to learn about the encoding.
    entry_types: u64,
    // A mask stating at which local keys (an integer between 0 and 31) an entry is exists.
    mask: u32,
    // The maximum number of entries this node can store.
    capacity: u8,
    // An artificial field ensuring the correct alignment of entries behind this header.
    __entries: [AlignmentStruct<K, V, IS, H>, ..0],
}

// A temporary reference to a node entries content. This is a safe wrapper around the unsafe,
// low-level bitmask-based memory representation of node entries.
enum NodeEntryRef<'a, K, V, IS, H> {
    CollisionEntryRef(&'a Arc<Vec<IS>>),
    ItemEntryRef(&'a IS),
    SubTreeEntryRef(&'a NodeRef<K, V, IS, H>)
}

impl<'a, K: Send+Share, V: Send+Share, IS: ItemStore<K, V>, H> NodeEntryRef<'a, K, V, IS, H> {
    // Clones the contents of a NodeEntryRef into a NodeEntryOwned value to be used elsewhere.
    fn clone_out(&self) -> NodeEntryOwned<K, V, IS, H> {
        match *self {
            CollisionEntryRef(r) => CollisionEntryOwned(r.clone()),
            ItemEntryRef(is) => ItemEntryOwned(is.clone()),
            SubTreeEntryRef(r) => SubTreeEntryOwned(r.clone()),
        }
    }
}

// The same as NodeEntryRef but allowing for mutable access to the referenced node entry.
enum NodeEntryMutRef<'a, K, V, IS, H> {
    CollisionEntryMutRef(&'a mut Arc<Vec<IS>>),
    ItemEntryMutRef(&'a mut IS),
    SubTreeEntryMutRef(&'a mut NodeRef<K, V, IS, H>)
}

// Similar to NodeEntryRef, but actually owning the entry data, so it can be moved around.
enum NodeEntryOwned<K, V, IS, H> {
    CollisionEntryOwned(Arc<Vec<IS>>),
    ItemEntryOwned(IS),
    SubTreeEntryOwned(NodeRef<K, V, IS, H>)
}

// This datatype is used to communicate between consecutive tree-levels about what to do when
// a change has occured below. When removing something from a subtree it sometimes makes sense to
// remove the entire subtree and replace it with a directly contained key-value pair in order to
// safe space and---later on during searches---time.
enum RemovalResult<K, V, IS, H> {
    // Don't do anything
    NoChange,
    // Replace the sub-tree entry with another sub-tree entry pointing to the given node
    ReplaceSubTree(NodeRef<K, V, IS, H>),
    // Collapse the sub-tree into a singe-item entry
    CollapseSubTree(IS),
    // Completely remove the entry
    KillSubTree
}

// impl UnsafeNode
impl<K, V, IS, H> UnsafeNode<K, V, IS, H> {

    // Retrieve the type code of the entry with the given index. Is always one of
    // {KVP_ENTRY, SUBTREE_ENTRY, COLLISION_ENTRY}
    fn get_entry_type_code(&self, index: uint) -> uint {
        assert!(index < self.entry_count());
        let type_code = ((self.entry_types >> (index * 2)) & 0b11) as uint;
        assert!(type_code != INVALID_ENTRY);
        type_code
    }

    // Set the type code of the entry with the given index. Must be one of
    // {KVP_ENTRY, SUBTREE_ENTRY, COLLISION_ENTRY}
    fn set_entry_type_code(&mut self, index: uint, type_code: uint) {
        assert!(index < self.entry_count());
        assert!(type_code <= 0b11 && type_code != INVALID_ENTRY);
        self.entry_types = (self.entry_types & !(0b11 << (index * 2))) |
                           (type_code as u64 << (index * 2));
    }

    // Get a raw pointer the an entry.
    fn get_entry_ptr(&self, index: uint) -> *u8 {
        assert!(index < self.entry_count());
        unsafe {
            let base: *u8 = mem::transmute(&self.__entries);
            base.offset((index * UnsafeNode::<K, V, IS, H>::node_entry_size()) as int)
        }
    }

    // Get a temporary, readonly reference to a node entry.
    fn get_entry<'a>(&'a self, index: uint) -> NodeEntryRef<'a, K, V, IS, H> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => ItemEntryRef(mem::transmute(entry_ptr)),
                SUBTREE_ENTRY => SubTreeEntryRef(mem::transmute(entry_ptr)),
                COLLISION_ENTRY => CollisionEntryRef(mem::transmute(entry_ptr)),
                _ => fail!("Invalid entry type code")
            }
        }
    }

    // Get a temporary, mutable reference to a node entry.
    fn get_entry_mut<'a>(&'a mut self, index: uint) -> NodeEntryMutRef<'a, K, V, IS, H> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => ItemEntryMutRef(mem::transmute(entry_ptr)),
                SUBTREE_ENTRY => SubTreeEntryMutRef(mem::transmute(entry_ptr)),
                COLLISION_ENTRY => CollisionEntryMutRef(mem::transmute(entry_ptr)),
                _ => fail!("Invalid entry type code")
            }
        }
    }

    // Initialize the entry with the given data. This will set the correct type code for the entry
    // move the given value to the correct memory position. It will not modify the nodes entry mask.
    fn init_entry<'a>(&mut self, index: uint, entry: NodeEntryOwned<K, V, IS, H>) {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match entry {
                ItemEntryOwned(kvp) => {
                    intrinsics::move_val_init(mem::transmute(entry_ptr), kvp);
                    self.set_entry_type_code(index, KVP_ENTRY);
                }
                SubTreeEntryOwned(node_ref) => {
                    intrinsics::move_val_init(mem::transmute(entry_ptr), node_ref);
                    self.set_entry_type_code(index, SUBTREE_ENTRY);
                }
                CollisionEntryOwned(arc) => {
                    intrinsics::move_val_init(mem::transmute(entry_ptr), arc);
                    self.set_entry_type_code(index, COLLISION_ENTRY);
                }
            }
        }
    }

    // The current number of entries stored in the node. Always <= the node's capacity.
    fn entry_count(&self) -> uint {
        bit_count(self.mask)
    }

    // The size in bytes of one node entry, containing any necessary padding bytes.
    fn node_entry_size() -> uint {
        ::std::cmp::max(
            mem::size_of::<IS>(),
            ::std::cmp::max(
                mem::size_of::<Arc<~[IS]>>(),
                mem::size_of::<NodeRef<K, V, IS, H>>(),
            )
        )
    }

    // Allocates a new node instance with the given mask and capacity. The memory for the node is
    // allocated from the exchange heap. The capacity of the node is fixed here on after.
    // The entries (including the entry_types bitfield) is not initialized by this call. Entries
    // must be initialized properly with init_entry() after allocation.
    fn alloc(mask: u32, capacity: uint) -> NodeRef<K, V, IS, H> {
        assert!(size_of_zero_entry_array::<K, V, IS, H>() == 0);
        fn size_of_zero_entry_array<K, V, IS, H>() -> uint {
            let node: UnsafeNode<K, V, IS, H> = UnsafeNode {
                ref_count: AtomicUint::new(0),
                entry_types: 0,
                mask: 0,
                capacity: 0,
                __entries: [],
            };

            mem::size_of_val(&node.__entries)
        }

        let align = mem::pref_align_of::<AlignmentStruct<K, V, IS, H>>();
        let entry_count = bit_count(mask);
        assert!(entry_count <= capacity);

        let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS, H>>(), align);
        let node_size = header_size + capacity * UnsafeNode::<K, V, IS, H>::node_entry_size();

        unsafe {
            let node_ptr: *mut UnsafeNode<K, V, IS, H> = mem::transmute(heap::allocate(node_size, align));
            intrinsics::move_val_init(&mut (*node_ptr).ref_count, AtomicUint::new(1));
            intrinsics::move_val_init(&mut (*node_ptr).entry_types, 0);
            intrinsics::move_val_init(&mut (*node_ptr).mask, mask);
            intrinsics::move_val_init(&mut (*node_ptr).capacity, capacity as u8);
            NodeRef { ptr: node_ptr }
        }
    }

    // Destroy the given node by first `dropping` all contained entries and then free the node's
    // memory.
    fn destroy(&mut self) {
        unsafe {
            for i in range(0, self.entry_count()) {
                self.drop_entry(i)
            }

            let align = mem::pref_align_of::<AlignmentStruct<K, V, IS, H>>();
            let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS, H>>(), align);
            let node_size = header_size + (self.capacity as uint) * UnsafeNode::<K, V, IS, H>::node_entry_size();

            heap::deallocate(mem::transmute(self), node_size, align);
        }
    }
    // CollisionEntryMutRef(&'a mut Arc<~[IS]>),
    // ItemEntryMutRef(&'a mut IS),
    // SubTreeEntryMutRef(&'a mut NodeRef<K, V, IS, H>)

    // Drops a single entry. Does not modify the entry_types or mask field of the node, just calls
    // the destructor of the entry at the given index.
    unsafe fn drop_entry(&mut self, index: uint) {
        // destroy the contained object, trick from Rc
        match self.get_entry_mut(index) {
            ItemEntryMutRef(item_ref) => {
                let _ = ptr::read(item_ref as *mut IS as *IS);
            }
            CollisionEntryMutRef(item_ref) => {
                let _ = ptr::read(item_ref as *mut Arc<Vec<IS>> as *Arc<Vec<IS>>);
            }
            SubTreeEntryMutRef(item_ref) => {
                let _ = ptr::read(item_ref as *mut NodeRef<K, V, IS, H> as *NodeRef<K, V, IS, H>);
            }
        }
    }
}

// impl UnsafeNode (continued)
impl<K: Eq+Send+Share+Hash<S>, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>>
UnsafeNode<K, V, IS, H> {
    // Insert a new key-value pair into the tree. The existing tree is not modified and a new tree
    // is created. This new tree will share most nodes with the existing one.
    fn insert(&self,
              // The *remaining* hash value. For every level down the tree, this value is shifted
              // to the right by `BITS_PER_LEVEL`
              hash: u64,
              // The hasher used for rehashing
              hasher: &H,
              // The current level of the tree
              level: uint,
              // The key-value pair to be inserted
              new_kvp: IS,
              // The number of newly inserted items. Must be set to either 0 (if an existing item is
              // replaced) or 1 (if there was not item with the given key yet). Used to keep track
              // of the trees total item count
              insertion_count: &mut uint)
              // Reference to the new tree containing the inserted element
           -> NodeRef<K, V, IS, H> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            let new_node = self.copy_with_new_entry(local_key, ItemEntryOwned(new_kvp));
            return new_node;
        }

        let index = get_index(self.mask, local_key);

        match self.get_entry(index) {
            ItemEntryRef(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    self.copy_with_new_entry(local_key, ItemEntryOwned(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = hasher.hash(existing_key) >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    self.copy_with_new_entry(local_key, SubTreeEntryOwned(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = vec!(new_kvp, existing_kvp_ref.clone());
                    self.copy_with_new_entry(local_key, CollisionEntryOwned(Arc::new(items)))
                }
            }
            CollisionEntryRef(items_arc) => {
                assert!(level == LAST_LEVEL);
                let items = items_arc.deref();
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = Vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.push_all(items.as_slice());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = Vec::with_capacity(item_count);

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

                self.copy_with_new_entry(local_key, CollisionEntryOwned(Arc::new(new_items)))
            }
            SubTreeEntryRef(sub_tree_ref) => {
                let new_sub_tree = sub_tree_ref.borrow().insert(hash >> BITS_PER_LEVEL,
                                                                hasher,
                                                                level + 1,
                                                                new_kvp,
                                                                insertion_count);

                self.copy_with_new_entry(local_key, SubTreeEntryOwned(new_sub_tree))
            }
        }
    }

    // Same as `insert()` above, but will do the insertion in-place (i.e. without copying) if the
    // node has enough capacity. Note, that we already have made sure at this point that there is
    // only exactly one reference to the node (otherwise we wouldn't have `&mut self`), so it is
    // safe to modify it in-place.
    // If there is not enough space for a new entry, then fall back to copying via a regular call
    // to `insert()`.
    fn try_insert_in_place(&mut self,
                           hash: u64,
                           hasher: &H,
                           level: uint,
                           new_kvp: IS,
                           insertion_count: &mut uint)
                        -> Option<NodeRef<K, V, IS, H>> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            if self.entry_count() < self.capacity as uint {
                // If yes, then fill it with a single-item entry
                *insertion_count = 1;
                self.insert_entry_in_place(local_key, ItemEntryOwned(new_kvp));
                return None;
            } else {
                // else fall back to copying
                return Some(self.insert(hash, hasher, level, new_kvp, insertion_count));
            }
        }

        let index = get_index(self.mask, local_key);

        // If there is no space left in this node but we would need it, again fall back to copying
        if self.entry_count() == self.capacity as uint &&
           self.get_entry_type_code(index) != SUBTREE_ENTRY {
            return Some(self.insert(hash, hasher, level, new_kvp, insertion_count));
        }

        let new_entry = match self.get_entry_mut(index) {
            ItemEntryMutRef(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    Some(ItemEntryOwned(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = hasher.hash(existing_key) >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. replace the ItemEntryRef entry with the subtree entry
                    Some(SubTreeEntryOwned(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = vec!(new_kvp, existing_kvp_ref.clone());
                    let collision_entry = CollisionEntryOwned(Arc::new(items));
                    Some(collision_entry)
                }
            }
            CollisionEntryMutRef(items) => {
                assert!(level == LAST_LEVEL);
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = Vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.push_all(items.as_slice());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = Vec::with_capacity(item_count);

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

                Some(CollisionEntryOwned(Arc::new(new_items)))
            }
            SubTreeEntryMutRef(subtree_mut_ref) => {
                match subtree_mut_ref.try_borrow_owned() {
                    SharedNode(subtree) => {
                        Some(SubTreeEntryOwned(subtree.insert(hash >> BITS_PER_LEVEL,
                                               hasher,
                                               level + 1,
                                               new_kvp,
                                               insertion_count)))
                    }
                    OwnedNode(subtree) => {
                        match subtree.try_insert_in_place(hash >> BITS_PER_LEVEL,
                                                          hasher,
                                                          level + 1,
                                                          new_kvp.clone(),
                                                          insertion_count) {
                            Some(new_sub_tree) => Some(SubTreeEntryOwned(new_sub_tree)),
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

    // Remove the item with the given key from the tree. Parameters correspond to this of
    // `insert()`. The result tells the call (the parent level in the tree) what it should do.
    fn remove(&self,
              hash: u64,
              level: uint,
              key: &K,
              removal_count: &mut uint)
           -> RemovalResult<K, V, IS, H> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if (self.mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return NoChange;
        }

        let index = get_index(self.mask, local_key);

        match self.get_entry(index) {
            ItemEntryRef(existing_kvp_ref) => {
                if *existing_kvp_ref.key() == *key {
                    *removal_count = 1;
                    self.collapse_kill_or_change(local_key, index)
                } else {
                    *removal_count = 0;
                    NoChange
                }
            }
            CollisionEntryRef(items_arc) => {
                assert!(level == LAST_LEVEL);
                let items = items_arc.deref();
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
                            let mut new_items = Vec::with_capacity(item_count);

                            if position > 0 {
                                new_items.push_all(items.slice_to(position));
                            }
                            if position < item_count - 1 {
                                new_items.push_all(items.slice_from(position + 1));
                            }
                            assert!(new_items.len() == item_count);

                            CollisionEntryOwned(Arc::new(new_items))
                        } else {
                            assert!(items.len() == 2);
                            assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = items.get(index_of_remaining_item).clone();

                            ItemEntryOwned(kvp)
                        };

                        let new_sub_tree = self.copy_with_new_entry(local_key, new_entry);
                        ReplaceSubTree(new_sub_tree)
                    }
                }
            }
            SubTreeEntryRef(sub_tree_ref) => {
                let result = sub_tree_ref.borrow().remove(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          key,
                                                          removal_count);
                match result {
                    NoChange => NoChange,
                    ReplaceSubTree(x) => {
                        ReplaceSubTree(self.copy_with_new_entry(local_key, SubTreeEntryOwned(x)))
                    }
                    CollapseSubTree(kvp) => {
                        if bit_count(self.mask) == 1 {
                            CollapseSubTree(kvp)
                        } else {
                            ReplaceSubTree(self.copy_with_new_entry(local_key, ItemEntryOwned(kvp)))
                        }
                    },
                    KillSubTree => {
                        self.collapse_kill_or_change(local_key, index)
                    }
                }
            }
        }
    }

    // Same as `remove()` but will do the modification in-place. As with `try_insert_in_place()` we
    // already have made sure at this point that there is only exactly one reference to the node
    // (otherwise we wouldn't have `&mut self`), so it is safe to modify it in-place.
    fn remove_in_place(&mut self,
                       hash: u64,
                       level: uint,
                       key: &K,
                       removal_count: &mut uint)
                    -> RemovalResult<K, V, IS, H> {
        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if (self.mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return NoChange;
        }

        let index = get_index(self.mask, local_key);

        enum Action<K, V, IS, H> {
            CollapseKillOrChange,
            NoAction,
            ReplaceEntry(NodeEntryOwned<K, V, IS, H>)
        }

        let action: Action<K, V, IS, H> = match self.get_entry_mut(index) {
            ItemEntryMutRef(existing_kvp_ref) => {
                if *existing_kvp_ref.key() == *key {
                    *removal_count = 1;
                    CollapseKillOrChange
                } else {
                    *removal_count = 0;
                    NoAction
                }
            }
            CollisionEntryMutRef(items) => {
                assert!(level == LAST_LEVEL);
                let position = items.iter().position(|kvp| *kvp.key() == *key);

                match position {
                    None => {
                        *removal_count = 0;
                        NoAction
                    },
                    Some(position) => {
                        *removal_count = 1;
                        let item_count = items.len() - 1;

                        // The new entry can either still be a collision node, or it can be a simple
                        // single-item node if the hash collision has been resolved by the removal
                        let new_entry = if item_count > 1 {
                            let mut new_items = Vec::with_capacity(item_count);

                            if position > 0 {
                                new_items.push_all(items.slice_to(position));
                            }
                            if position < item_count - 1 {
                                new_items.push_all(items.slice_from(position + 1));
                            }
                            assert!(new_items.len() == item_count);

                            CollisionEntryOwned(Arc::new(new_items))
                        } else {
                            assert!(items.len() == 2);
                            assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = items.get(index_of_remaining_item).clone();

                            ItemEntryOwned(kvp)
                        };

                        ReplaceEntry(new_entry)
                    }
                }
            }
            SubTreeEntryMutRef(sub_tree_ref) => {
                let result = match sub_tree_ref.try_borrow_owned() {
                    SharedNode(node_ref) => node_ref.remove(hash >> BITS_PER_LEVEL,
                                                            level + 1,
                                                            key,
                                                            removal_count),
                    OwnedNode(node_ref) => node_ref.remove_in_place(hash >> BITS_PER_LEVEL,
                                                                    level + 1,
                                                                    key,
                                                                    removal_count)
                };

                match result {
                    NoChange => NoAction,
                    ReplaceSubTree(x) => {
                        ReplaceEntry(SubTreeEntryOwned(x))
                    }
                    CollapseSubTree(kvp) => {
                        if bit_count(self.mask) == 1 {
                            return CollapseSubTree(kvp);
                        }

                        ReplaceEntry(ItemEntryOwned(kvp))
                    },
                    KillSubTree => {
                        CollapseKillOrChange
                    }
                }
            }
        };

        match action {
            NoAction => NoChange,
            CollapseKillOrChange => self.collapse_kill_or_change_in_place(local_key, index),
            ReplaceEntry(new_entry) => {
                self.insert_entry_in_place(local_key, new_entry);
                NoChange
            }
        }
    }

    // Determines how the parent node should handle the removal of the entry at local_key from this
    // node.
    fn collapse_kill_or_change(&self,
                               local_key: uint,
                               entry_index: uint)
                            -> RemovalResult<K, V, IS, H> {
        let new_entry_count = bit_count(self.mask) - 1;

        if new_entry_count > 1 {
            ReplaceSubTree(self.copy_without_entry(local_key))
        } else if new_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.get_entry(other_index) {
                ItemEntryRef(kvp_ref) => {
                    CollapseSubTree(kvp_ref.clone())
                }
                _ => ReplaceSubTree(self.copy_without_entry(local_key))
            }
        } else {
            assert!(new_entry_count == 0);
            KillSubTree
        }
    }

    // Same as `collapse_kill_or_change()` but will do the modification in-place.
    fn collapse_kill_or_change_in_place(&mut self,
                                        local_key: uint,
                                        entry_index: uint)
                                     -> RemovalResult<K, V, IS, H> {
        let new_entry_count = self.entry_count() - 1;

        if new_entry_count > 1 {
            self.remove_entry_in_place(local_key);
            NoChange
        } else if new_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.get_entry(other_index) {
                ItemEntryRef(kvp_ref) => {
                    return CollapseSubTree(kvp_ref.clone())
                }
                _ => { /* */ }
            }

            self.remove_entry_in_place(local_key);
            NoChange
        } else {
            assert!(new_entry_count == 0);
            KillSubTree
        }
    }

    // Copies this node with a new entry at `local_key`. Might replace an old entry.
    fn copy_with_new_entry(&self,
                           local_key: uint,
                           new_entry: NodeEntryOwned<K, V, IS, H>)
                        -> NodeRef<K, V, IS, H> {
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

    // Inserts a new node entry in-place. Will take care of modifying node entry data, including the
    // node's mask and entry_types fields.
    fn insert_entry_in_place(&mut self,
                             local_key: uint,
                             new_entry: NodeEntryOwned<K, V, IS, H>) {
        let new_mask: u32 = self.mask | (1 << local_key);
        let replace_old_entry = new_mask == self.mask;
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
                    let dest: *mut u8 = mem::transmute(
                        source.offset(UnsafeNode::<K, V, IS, H>::node_entry_size() as int));
                    let count = (self.entry_count() - index) *
                        UnsafeNode::<K, V, IS, H>::node_entry_size();
                    ptr::copy_memory(dest, source, count);

                    let type_mask_up_to_index: u64 = 0xFFFFFFFFFFFFFFFFu64 << ((index + 1) * 2);
                    self.entry_types = ((self.entry_types << 2) & type_mask_up_to_index) |
                                       (self.entry_types & !type_mask_up_to_index);
                }
            }

            self.mask = new_mask;
            self.init_entry(index, new_entry);
        }
    }

    // Given that the current capacity is too small, returns how big the new node should be.
    fn expanded_capacity(&self) -> uint {
        if self.capacity == 0 {
            MIN_CAPACITY
        } else if self.capacity > 16 {
            32
        } else {
            ((self.capacity as uint) * 2)
        }
    }

    // Create a copy of this node which does not contain the entry at 'local_key'.
    fn copy_without_entry(&self, local_key: uint) -> NodeRef<K, V, IS, H> {
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

    // Same as `copy_without_entry()` but applies the modification in place.
    fn remove_entry_in_place(&mut self, local_key: uint) {
        assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let index = get_index(self.mask, local_key);

        unsafe {
            self.drop_entry(index);

            if index < self.entry_count() - 1 {
                let source: *u8 = self.get_entry_ptr(index + 1);
                let dest: *mut u8 = mem::transmute(
                    source.offset(-(UnsafeNode::<K, V, IS, H>::node_entry_size() as int))
                    );
                let count = (self.entry_count() - (index + 1)) *
                    UnsafeNode::<K, V, IS, H>::node_entry_size();
                ptr::copy_memory(dest, source, count);

                let type_mask_up_to_index: u64 = 0xFFFFFFFFFFFFFFFFu64 << ((index + 1) * 2);
                self.entry_types = ((self.entry_types & type_mask_up_to_index) >> 2) |
                                   (self.entry_types & !(type_mask_up_to_index >> 2));
            }
        }

        self.mask = new_mask;
    }

    // Creates a new node with containing the two given items and MIN_CAPACITY. Might create a
    // whole subtree if the hash values of the two items necessitate it.
    fn new_with_entries(new_kvp: IS,
                        new_hash: u64,
                        existing_kvp: &IS,
                        existing_hash: u64,
                        level: uint)
                     -> NodeRef<K, V, IS, H> {
        assert!(level <= LAST_LEVEL);

        let new_local_key = new_hash & LEVEL_BIT_MASK;
        let existing_local_key = existing_hash & LEVEL_BIT_MASK;

        if new_local_key != existing_local_key {
            let mask = (1 << new_local_key) | (1 << existing_local_key);
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();

                if new_local_key < existing_local_key {
                    new_node.init_entry(0, ItemEntryOwned(new_kvp));
                    new_node.init_entry(1, ItemEntryOwned(existing_kvp.clone()));
                } else {
                    new_node.init_entry(0, ItemEntryOwned(existing_kvp.clone()));
                    new_node.init_entry(1, ItemEntryOwned(new_kvp));
                };
            }
            new_node_ref
        } else if level == LAST_LEVEL {
            let mask = 1 << new_local_key;
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();
                let items = vec!(new_kvp, existing_kvp.clone());
                new_node.init_entry(0, CollisionEntryOwned(Arc::new(items)));
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
                new_node.init_entry(0, SubTreeEntryOwned(sub_tree));
            }
            new_node_ref
        }
    }
}



//=-------------------------------------------------------------------------------------------------
// HamtMap
//=-------------------------------------------------------------------------------------------------
pub struct HamtMap<K, V, IS, H> {
    root: NodeRef<K, V, IS, H>,
    hasher: H,
    element_count: uint,
}

// impl HamtMap
impl<K: Eq+Send+Share+Hash<S>, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>+Clone>
HamtMap<K, V, IS, H> {

    pub fn new(hasher: H) -> HamtMap<K, V, IS, H> {
        HamtMap {
            root: UnsafeNode::alloc(0, 0),
            hasher: hasher,
            element_count: 0
        }
    }

    pub fn iter<'a>(&'a self) -> HamtMapIterator<'a, K, V, IS, H> {
        HamtMapIterator::new(self)
    }

    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        // let mut hash = key.hash();
        let mut hash = self.hasher.hash(key);

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
                ItemEntryRef(kvp_ref) => return if *key == *kvp_ref.key() {
                    Some(kvp_ref.val())
                } else {
                    None
                },
                CollisionEntryRef(items) => {
                    assert!(level == LAST_LEVEL);
                    let found = items.iter().find(|&kvp| *key == *kvp.key());
                    return match found {
                        Some(kvp) => Some(kvp.val()),
                        None => None,
                    };
                }
                SubTreeEntryRef(subtree_ref) => {
                    assert!(level < LAST_LEVEL);
                    current_node = subtree_ref.borrow();
                    hash = hash >> BITS_PER_LEVEL;
                    level += 1;
                }
            };
        }
    }

    fn insert_internal(self, kvp: IS) -> (HamtMap<K, V, IS, H>, bool) {
        let HamtMap { mut root, hasher, element_count } = self;
        let hash = hasher.hash(kvp.key());
        let mut insertion_count = 0xdeadbeaf;

        // If we hold the only reference to the root node, then try to insert the KVP in-place
        let new_root = match root.try_borrow_owned() {
            OwnedNode(mutable) => mutable.try_insert_in_place(hash, &hasher, 0, kvp, &mut insertion_count),
            SharedNode(immutable) => Some(immutable.insert(hash, &hasher, 0, kvp, &mut insertion_count))
        };

        // Make sure that insertion_count was set properly
        assert!(insertion_count != 0xdeadbeaf);

        match new_root {
            Some(r) => (
                HamtMap {
                    root: r,
                    hasher: hasher,
                    element_count: element_count + insertion_count
                },
                insertion_count != 0
            ),
            None => (
                HamtMap {
                    root: root,
                    hasher: hasher,
                    element_count: element_count + insertion_count
                },
                insertion_count != 0
            )
        }
    }

    fn try_remove_in_place(self, key: &K) -> (HamtMap<K, V, IS, H>, bool) {
        let HamtMap { mut root, hasher, element_count } = self;
        let hash = hasher.hash(key);
        let mut removal_count = 0xdeadbeaf;

        let removal_result = match root.try_borrow_owned() {
            SharedNode(node_ref) => node_ref.remove(hash, 0, key, &mut removal_count),
            OwnedNode(node_ref) => node_ref.remove_in_place(hash, 0, key, &mut removal_count)
        };
        assert!(removal_count != 0xdeadbeaf);
        let new_element_count = element_count - removal_count;

        (match removal_result {
            NoChange => HamtMap {
                root: root,
                hasher: hasher,
                element_count: new_element_count
            },
            ReplaceSubTree(new_root) => HamtMap {
                root: new_root,
                hasher: hasher,
                element_count: new_element_count
            },
            CollapseSubTree(kvp) => {
                assert!(bit_count(root.borrow().mask) == 2);
                let local_key = (hasher.hash(kvp.key()) & LEVEL_BIT_MASK) as uint;

                let mask = 1 << local_key;
                let mut new_root_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
                {
                    let root = new_root_ref.borrow_mut();
                    root.init_entry(0, ItemEntryOwned(kvp));
                }
                HamtMap {
                    root: new_root_ref,
                    hasher: hasher,
                    element_count: new_element_count
                }
            }
            KillSubTree => {
                assert!(bit_count(root.borrow().mask) == 1);
                HamtMap::new(hasher)
            }
        }, removal_count != 0)
    }
}

// Clone for HamtMap
impl<K: Eq+Send+Share, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>+Clone>
Clone for HamtMap<K, V, IS, H> {

    fn clone(&self) -> HamtMap<K, V, IS, H> {
        HamtMap {
            root: self.root.clone(),
            hasher: self.hasher.clone(),
            element_count: self.element_count
        }
    }
}

// Container for HamtMap
impl<K: Eq+Send+Share, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>>
Container for HamtMap<K, V, IS, H> {

    fn len(&self) -> uint {
        self.element_count
    }
}

// Map for HamtMap
impl<K: Eq+Send+Share+Hash<S>, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>+Clone>
Map<K, V> for HamtMap<K, V, IS, H> {

    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        self.find(key)
    }
}

// PersistentMap for HamtMap<CopyStore>
impl<K: Eq+Send+Share+Clone+Hash<S>, V: Send+Share+Clone, S, H: Hasher<S>+Clone>
PersistentMap<K, V> for HamtMap<K, V, CopyStore<K, V>, H> {

    fn insert(self, key: K, value: V) -> (HamtMap<K, V, CopyStore<K, V>, H>, bool) {
        self.insert_internal(CopyStore::new(key, value))
    }

    fn remove(self, key: &K) -> (HamtMap<K, V, CopyStore<K, V>, H>, bool) {
        self.try_remove_in_place(key)
    }
}

// PersistentMap for HamtMap<ShareStore>
impl<K: Eq+Send+Share+Hash<S>, V: Send+Share, S, H: Hasher<S>+Clone>
PersistentMap<K, V> for HamtMap<K, V, ShareStore<K, V>, H> {

    fn insert(self, key: K, value: V) -> (HamtMap<K, V, ShareStore<K, V>, H>, bool) {
        self.insert_internal(ShareStore::new(key,value))
    }

    fn remove(self, key: &K) -> (HamtMap<K, V, ShareStore<K, V>, H>, bool) {
        self.try_remove_in_place(key)
    }
}



//=-------------------------------------------------------------------------------------------------
// HamtMapIterator
//=-------------------------------------------------------------------------------------------------

enum IteratorNodeRef<'a, K, V, IS, H> {
    IterNodeRef(&'a UnsafeNode<K, V, IS, H>),
    IterCollisionEntryRef(&'a Vec<IS>),
    IterEmpty
}

pub struct HamtMapIterator<'a, K, V, IS, H> {
    node_stack: [(IteratorNodeRef<'a, K, V, IS, H>, int), .. LAST_LEVEL + 2],
    stack_size: uint,
    len: uint,
}

impl<'a, K: Eq+Send+Share, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>>
HamtMapIterator<'a, K, V, IS, H> {

    fn new<'a>(map: &'a HamtMap<K, V, IS, H>) -> HamtMapIterator<'a, K, V, IS, H> {
        let mut iterator = HamtMapIterator {
            node_stack: unsafe{ intrinsics::uninit() },
            stack_size: 1,
            len: map.element_count,
        };

        iterator.node_stack[0] = (IterNodeRef(map.root.borrow()), -1);
        iterator
    }
}

impl<'a, K: Eq+Send+Share, V: Send+Share, IS: ItemStore<K, V>, S, H: Hasher<S>>
Iterator<(&'a K, &'a V)> for HamtMapIterator<'a, K, V, IS, H> {

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.stack_size == 0 {
            return None;
        }

        let (current_node, index) = self.node_stack[self.stack_size - 1];
        let next_index: uint = (index + 1) as uint;

        match current_node {
            IterNodeRef(node_ref) => {
                if next_index == node_ref.entry_count() {
                    self.stack_size -= 1;
                    return self.next();
                } else {
                    let (_, ref mut stack_index) = self.node_stack[self.stack_size - 1];
                    *stack_index = next_index as int;
                }

                match node_ref.get_entry(next_index) {
                    ItemEntryRef(item_ref) => {
                        return Some((item_ref.key(), item_ref.val()));
                    }
                    CollisionEntryRef(items_arc) => {
                        let items = items_arc.deref();
                        self.node_stack[self.stack_size] = (IterCollisionEntryRef(items), 0);
                        self.stack_size += 1;
                        let item = &items.get(0);
                        return Some((item.key(), item.val()));
                    },
                    SubTreeEntryRef(subtree_ref) => {
                        self.node_stack[self.stack_size] = (IterNodeRef(subtree_ref.borrow()), -1);
                        self.stack_size += 1;
                        return self.next();
                    }
                };
            }
            IterCollisionEntryRef(items_ref) => {
                if next_index == items_ref.len() {
                    self.stack_size -= 1;
                    return self.next();
                }

                let item = &items_ref.get(next_index);
                return Some((item.key(), item.val()));
            }
            IterEmpty => unreachable!()
        }
    }

    fn size_hint(&self) -> (uint, Option<uint>) {
        (self.len, Some(self.len))
    }
}



//=-------------------------------------------------------------------------------------------------
// HamtSet
//=------------------------------------------------------------------------------------------------
struct HamtSet<V, H> {
    data: HamtMap<V, (), ShareStore<V, ()>, H>
}

// Set for HamtSet
impl<V: Send+Share+Eq+Hash<S>, S, H: Hasher<S>+Clone>
Set<V> for HamtSet<V, H> {
    fn contains(&self, value: &V) -> bool {
        self.data.contains_key(value)
    }

    fn is_disjoint(&self, other: &HamtSet<V, H>) -> bool {
        for (v, _) in self.data.iter() {
            if other.contains(v) {
                return false;
            }
        }

        return true;
    }

    fn is_subset(&self, other: &HamtSet<V, H>) -> bool {
        if self.len() > other.len() {
            return false;
        }

        for (v, _) in self.data.iter() {
            if !other.contains(v) {
                return false;
            }
        }

        return true;
    }

    fn is_superset(&self, other: &HamtSet<V, H>) -> bool {
        other.is_subset(self)
    }
}

// Clone for HamtSet
impl<V: Eq+Send+Share, S, H: Hasher<S>+Clone>
Clone for HamtSet<V, H> {

    fn clone(&self) -> HamtSet<V, H> {
        HamtSet {
            data: self.data.clone()
        }
    }
}

// Container for HamtSet
impl<V: Eq+Send+Share, S, H: Hasher<S>>
Container for HamtSet<V, H> {

    fn len(&self) -> uint {
        self.data.len()
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
    use std::num::Bitwise;
    x.count_ones() as uint
}

fn align_to(size: uint, align: uint) -> uint {
    assert!(align != 0 && bit_count(align as u32) == 1);
    (size + align - 1) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::get_index;
    use super::HamtMap;
    use testing::Test;
    use test::Bencher;
    use collections::hashmap::HashMap;
    use PersistentMap;
    use std::hash::sip::{SipHasher, SipState};

    type CopyStore = ::item_store::CopyStore<u64, u64>;
    type ShareStore = ::item_store::ShareStore<u64, u64>;

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

//=-------------------------------------------------------------------------------------------------
// Test HamtMap<CopyStore>
//=-------------------------------------------------------------------------------------------------

    #[test]
    fn test_iterator_copy() {
        let mut map: HamtMap<u64, u64, CopyStore, SipHasher> = HamtMap::new(SipHasher::new());
        let count = 1000u;

        for i in range(0u64, count as u64) {
            map = map.plus(i, i);
        }

        let it = map.iter();
        assert_eq!((count, Some(count)), it.size_hint());

        let reference: HashMap<u64, u64> = it.map(|(x, y)| (*x, *y)).collect();

        assert_eq!(count, reference.len());

        for i in range(0u64, count as u64) {
            assert_eq!(reference.find(&i), Some(&i));
        }
    }

    #[test]
    fn test_insert_copy() {
        Test::test_insert(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_ascending_copy() {
        Test::test_insert_ascending(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_descending_copy() {
        Test::test_insert_descending(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_overwrite_copy() {
        Test::test_insert_overwrite(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_remove_copy() {
        Test::test_remove(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn stress_test_copy() {
        Test::random_insert_remove_stress_test(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()));
    }



//=-------------------------------------------------------------------------------------------------
// Bench HamtMap<CopyStore>
//=-------------------------------------------------------------------------------------------------

    #[bench]
    fn bench_insert_copy_10(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_insert_copy_1000(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_insert_copy_100000(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_find_copy_10(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_find_copy_1000(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_find_copy_100000(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_remove_copy_10(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_remove_copy_1000(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_remove_copy_100000(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_iterate_copy_10(bh: &mut Bencher) {
        bench_iterator_copy(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_iterate_copy_1000(bh: &mut Bencher) {
        bench_iterator_copy(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_iterate_copy_100000(bh: &mut Bencher) {
        bench_iterator_copy(HamtMap::<u64, u64, CopyStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    fn bench_iterator_copy(mut map: HamtMap<u64, u64, CopyStore, SipHasher>,
                           size: uint,
                           bh: &mut Bencher) {
        for i in range(0u64, size as u64) {
            map = map.plus(i, i);
        }

        bh.iter(|| {
            for _ in map.iter() {}
        })
    }



//=-------------------------------------------------------------------------------------------------
// Test HamtMap<ShareStore>
//=-------------------------------------------------------------------------------------------------

    #[test]
    fn test_iterator_share() {
        let mut map: HamtMap<u64, u64, ShareStore, SipHasher> = HamtMap::new(SipHasher::new());
        let count = 1000u;

        for i in range(0u64, count as u64) {
            map = map.plus(i, i);
        }

        let it = map.iter();
        assert_eq!((count, Some(count)), it.size_hint());

        let test: HashMap<u64, u64> = it.map(|(x, y)| (*x, *y)).collect();

        assert_eq!(count, test.len());

        for i in range(0u64, count as u64) {
            assert_eq!(test.find(&i), Some(&i));
        }
    }

    #[test]
    fn test_insert_share() {
        Test::test_insert(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_ascending_share() {
        Test::test_insert_ascending(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_descending_share() {
        Test::test_insert_descending(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_insert_overwrite_share() {
        Test::test_insert_overwrite(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn test_remove_share() {
        Test::test_remove(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

    #[test]
    fn stress_test_share() {
        Test::random_insert_remove_stress_test(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()));
    }

//=-------------------------------------------------------------------------------------------------
// Bench HamtMap<ShareStore>
//=-------------------------------------------------------------------------------------------------

    #[bench]
    fn bench_insert_share_10(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_insert_share_1000(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_insert_share_100000(bh: &mut Bencher) {
        Test::bench_insert(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_find_share_10(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_find_share_1000(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_find_share_100000(bh: &mut Bencher) {
        Test::bench_find(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_remove_share_10(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_remove_share_1000(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_remove_share_100000(bh: &mut Bencher) {
        Test::bench_remove(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    #[bench]
    fn bench_iterate_share_10(bh: &mut Bencher) {
        bench_iterator_share(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 10, bh);
    }

    #[bench]
    fn bench_iterate_share_1000(bh: &mut Bencher) {
        bench_iterator_share(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 1000, bh);
    }

    #[bench]
    fn bench_iterate_share_100000(bh: &mut Bencher) {
        bench_iterator_share(HamtMap::<u64, u64, ShareStore, SipState, SipHasher>::new(SipHasher::new()), 100000, bh);
    }

    fn bench_iterator_share(mut map: HamtMap<u64, u64, ShareStore, SipHasher>,
                            size: uint,
                            bh: &mut Bencher) {
        for i in range(0u64, size as u64) {
            map = map.plus(i, i);
        }

        bh.iter(|| {
            for _ in map.iter() {}
        })
    }
}
