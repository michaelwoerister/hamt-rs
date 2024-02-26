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

//! A Hash Array Mapped Trie implementation based on the
//! [Ideal Hash Trees](http://lampwww.epfl.ch/papers/idealhashtrees.pdf) paper by Phil Bagwell.
//! This is the datastructure used by Scala's and Clojure's standard library as map implementation.
//! The idea to use a special *collision node* to deal with hash collisions is taken from Clojure's
//! implementation.


use std::hash::{Hasher, Hash};
use std::mem;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::default::Default;

use std::sync::Arc;
use item_store::{ItemStore, ShareStore};

use std::collections::hash_map::DefaultHasher as StdHasher;
use libc;

#[cfg(not(double_wide_nodes))]
type MaskType = u32;
#[cfg(not(double_wide_nodes))]
type EntryTypesType = u64;
#[cfg(not(double_wide_nodes))]
const ENTRY_TYPES_MAX: EntryTypesType = ::std::u64::MAX;

#[cfg(double_wide_nodes)]
type MaskType = u64;
#[cfg(double_wide_nodes)]
type EntryTypesType = u128;
#[cfg(double_wide_nodes)]
const ENTRY_TYPES_MAX: EntryTypesType = ::std::u128::MAX;

//=-------------------------------------------------------------------------------------------------
// NodeRef
//=-------------------------------------------------------------------------------------------------
// A smart pointer for handling node lifetimes, very similar to sync::Arc.
struct NodeRef<K, V, IS, H> {
    ptr: *mut UnsafeNode<K, V, IS, H>
}

// NodeRef knows if it is the only reference to a given node and can thus safely decide to allow for
// mutable access to the referenced node. This type indicates whether mutable access could be
// acquired.
enum BorrowedNodeRef<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    Exclusive(&'a mut UnsafeNode<K, V, IS, H>),
    Shared(&'a UnsafeNode<K, V, IS, H>),
}

impl<K, V, IS, H> NodeRef<K, V, IS, H>
    where K: Eq,
          IS: ItemStore<K, V>,
          H: Hasher
{
    fn borrow<'a>(&'a self) -> &'a UnsafeNode<K, V, IS, H> {
        unsafe {
            mem::transmute(self.ptr)
        }
    }

    fn borrow_mut<'a>(&'a mut self) -> &'a mut UnsafeNode<K, V, IS, H> {
        unsafe {
            debug_assert!((*self.ptr).ref_count.load(Ordering::SeqCst) == 1);
            mem::transmute(self.ptr)
        }
    }

    // Try to safely gain mutable access to the referenced node. This can be used to safely make
    // in-place modifications instead of unnecessarily copying data.
    fn try_borrow_owned<'a>(&'a mut self) -> BorrowedNodeRef<'a, K, V, IS, H> {
        unsafe {
            if (*self.ptr).ref_count.load(Ordering::SeqCst) == 1 {
                BorrowedNodeRef::Exclusive(mem::transmute(self.ptr))
            } else {
                BorrowedNodeRef::Shared(mem::transmute(self.ptr))
            }
        }
    }
}

impl<K, V, IS, H> Drop for NodeRef<K, V, IS, H> {
    fn drop(&mut self) {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS, H> = mem::transmute(self.ptr);
            let old_count = node.ref_count.fetch_sub(1, Ordering::SeqCst);
            debug_assert!(old_count >= 1);
            if old_count == 1 {
                node.destroy();
            }
        }
    }
}

impl<K, V, IS, H> Clone for NodeRef<K, V, IS, H> {
    fn clone(&self) -> NodeRef<K, V, IS, H> {
        unsafe {
            let node: &mut UnsafeNode<K, V, IS, H> = mem::transmute(self.ptr);
            let old_count = node.ref_count.fetch_add(1, Ordering::SeqCst);
            debug_assert!(old_count >= 1);
        }

        NodeRef { ptr: self.ptr }
    }
}



//=-------------------------------------------------------------------------------------------------
// UnsafeNode
//=-------------------------------------------------------------------------------------------------
// The number of hash-value bits used per tree-level.
const BITS_PER_LEVEL: usize = 5;
// The deepest level the tree can have. Collision-nodes are use at this depth to avoid any further
// recursion.
const LAST_LEVEL: usize = (64 / BITS_PER_LEVEL) - 1;
// Used to mask off any unused bits from the hash key at a given level.
const LEVEL_BIT_MASK: u64 = (1 << BITS_PER_LEVEL) - 1;
// The minimum node capacity.
const MIN_CAPACITY: usize = 4;

// This struct should have the correct alignment for node entries.
struct AlignmentStruct<K, V, IS, H> {
    _a: Arc<Vec<IS>>,
    _b: IS,
    _c: *const (),
    _k: ::std::marker::PhantomData<K>,
    _v: ::std::marker::PhantomData<V>,
    _h: ::std::marker::PhantomData<H>,
}

// Bit signature of node entry types. Every node contains a single u64 designating the kinds of all
// its entries, which can either be a key-value pair, a reference to a sub-tree, or a
// collision-entry, containing a linear list of colliding key-value pairs.
const KVP_ENTRY: usize = 0b01;
const SUBTREE_ENTRY: usize = 0b10;
const COLLISION_ENTRY: usize = 0b11;
const INVALID_ENTRY: usize = 0b00;

// The central node type used by the implementation. Note that this struct just represents the
// header of the node data. The actual entries are allocated directly after this header, starting
// at the address of the `__entries` field.
#[repr(C)]
struct UnsafeNode<K, V, IS, H> {
    // The current number of references to this node.
    ref_count: AtomicUsize,
    // The entry types of the of this node. Each two bits encode the type of one entry
    // (key-value pair, subtree ref, or collision entry). See get_entry_type_code() and the above
    // constants to learn about the encoding.
    entry_types: EntryTypesType,
    // A mask stating at which local keys (an integer between 0 and 31) an entry is exists.
    mask: MaskType,
    // The maximum number of entries this node can store.
    capacity: u8,
    // An artificial field ensuring the correct alignment of entries behind this header.
    __entries: [AlignmentStruct<K, V, IS, H>; 0],
}

// A temporary reference to a node entry's content. This is a safe wrapper around the unsafe,
// low-level bitmask-based memory representation of node entries.
enum NodeEntryRef<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    Collision(&'a Arc<Vec<IS>>),
    Item(&'a IS),
    SubTree(&'a NodeRef<K, V, IS, H>)
}

impl<'a, K, V, IS, H> NodeEntryRef<'a, K, V, IS, H>
    where IS: ItemStore<K, V>
{
    // Clones the contents of a NodeEntryRef into a NodeEntryOwned value to be used elsewhere.
    fn clone_out(&self) -> NodeEntryOwned<K, V, IS, H> {
        match *self {
            NodeEntryRef::Collision(r) => NodeEntryOwned::Collision(r.clone()),
            NodeEntryRef::Item(is) => NodeEntryOwned::Item(is.clone()),
            NodeEntryRef::SubTree(r) => NodeEntryOwned::SubTree(r.clone()),
        }
    }
}

// The same as NodeEntryRef but allowing for mutable access to the referenced node entry.
enum NodeEntryMutRef<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    Collision(&'a mut Arc<Vec<IS>>),
    Item(&'a mut IS),
    SubTree(&'a mut NodeRef<K, V, IS, H>)
}

// Similar to NodeEntryRef, but actually owning the entry data, so it can be moved around.
enum NodeEntryOwned<K, V, IS, H> {
    Collision(Arc<Vec<IS>>),
    Item(IS),
    SubTree(NodeRef<K, V, IS, H>)
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
impl<'a, K, V, IS, H> UnsafeNode<K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    // Retrieve the type code of the entry with the given index. Is always one of
    // {KVP_ENTRY, SUBTREE_ENTRY, COLLISION_ENTRY}
    fn get_entry_type_code(&self, index: usize) -> usize {
        debug_assert!(index < self.entry_count());
        let type_code = ((self.entry_types >> (index * 2)) & 0b11) as usize;
        debug_assert!(type_code != INVALID_ENTRY);
        type_code
    }

    // Set the type code of the entry with the given index. Must be one of
    // {KVP_ENTRY, SUBTREE_ENTRY, COLLISION_ENTRY}
    fn set_entry_type_code(&mut self, index: usize, type_code: usize) {
        debug_assert!(index < self.entry_count());
        debug_assert!(type_code <= 0b11 && type_code != INVALID_ENTRY);
        self.entry_types = (self.entry_types & !(0b11 << (index * 2))) |
                           ((type_code as EntryTypesType) << (index * 2));
    }

    // Get a raw pointer the an entry.
    fn get_entry_ptr(&self, index: usize) -> *const u8 {
        debug_assert!(index < self.entry_count());
        unsafe {
            let base: *const u8 = mem::transmute(&self.__entries);
            base.offset((index * UnsafeNode::<K, V, IS, H>::node_entry_size()) as isize)
        }
    }

    // Get a temporary, readonly reference to a node entry.
    fn get_entry(&'a self, index: usize) -> NodeEntryRef<'a, K, V, IS, H> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => NodeEntryRef::Item(mem::transmute(entry_ptr)),
                SUBTREE_ENTRY => NodeEntryRef::SubTree(mem::transmute(entry_ptr)),
                COLLISION_ENTRY => NodeEntryRef::Collision(mem::transmute(entry_ptr)),
                _ => panic!("Invalid entry type code")
            }
        }
    }

    // Get a temporary, mutable reference to a node entry.
    fn get_entry_mut(&'a mut self, index: usize) -> NodeEntryMutRef<'a, K, V, IS, H> {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match self.get_entry_type_code(index) {
                KVP_ENTRY => NodeEntryMutRef::Item(mem::transmute(entry_ptr)),
                SUBTREE_ENTRY => NodeEntryMutRef::SubTree(mem::transmute(entry_ptr)),
                COLLISION_ENTRY => NodeEntryMutRef::Collision(mem::transmute(entry_ptr)),
                _ => panic!("Invalid entry type code")
            }
        }
    }

    // Initialize the entry with the given data. This will set the correct type
    // code for the entry and move the given value to the correct memory
    // position. It will not modify the nodes entry mask.
    fn init_entry(&mut self, index: usize, entry: NodeEntryOwned<K, V, IS, H>) {
        let entry_ptr = self.get_entry_ptr(index);

        unsafe {
            match entry {
                NodeEntryOwned::Item(kvp) => {
                    ptr::write(mem::transmute(entry_ptr), kvp);
                    self.set_entry_type_code(index, KVP_ENTRY);
                }
                NodeEntryOwned::SubTree(node_ref) => {
                    ptr::write(mem::transmute(entry_ptr), node_ref);
                    self.set_entry_type_code(index, SUBTREE_ENTRY);
                }
                NodeEntryOwned::Collision(arc) => {
                    ptr::write(mem::transmute(entry_ptr), arc);
                    self.set_entry_type_code(index, COLLISION_ENTRY);
                }
            }
        }
    }

    // The current number of entries stored in the node. Always <= the node's capacity.
    fn entry_count(&self) -> usize {
        bit_count(self.mask)
    }

    // The size in bytes of one node entry, containing any necessary padding bytes.
    fn node_entry_size() -> usize {
        ::std::cmp::max(
            mem::size_of::<IS>(),
            ::std::cmp::max(
                mem::size_of::<Arc<Vec<IS>>>(),
                mem::size_of::<NodeRef<K, V, IS, H>>(),
            )
        )
    }

    // Allocates a new node instance with the given mask and capacity. The memory for the node is
    // allocated from the exchange heap. The capacity of the node is fixed from here on after.
    // The entries (including the entry_types bitfield) is not initialized by this call. Entries
    // must be initialized properly with init_entry() after allocation.
    fn alloc(mask: MaskType, capacity: usize) -> NodeRef<K, V, IS, H> {
        debug_assert!(size_of_zero_entry_array::<K, V, IS, H>() == 0);
        fn size_of_zero_entry_array<K, V, IS, H>() -> usize {
            let node: UnsafeNode<K, V, IS, H> = UnsafeNode {
                ref_count: AtomicUsize::new(0),
                entry_types: 0,
                mask: 0,
                capacity: 0,
                __entries: [],
            };

            mem::size_of_val(&node.__entries)
        }

        let align = mem::align_of::<AlignmentStruct<K, V, IS, H>>();
        let entry_count = bit_count(mask);
        debug_assert!(entry_count <= capacity);

        let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS, H>>(), align);
        let node_size = header_size + capacity * UnsafeNode::<K, V, IS, H>::node_entry_size();

        unsafe {
            let node_ptr: *mut UnsafeNode<K, V, IS, H> = mem::transmute(allocate(node_size, align));
            ptr::write(&mut (*node_ptr).ref_count, AtomicUsize::new(1));
            ptr::write(&mut (*node_ptr).entry_types, 0);
            ptr::write(&mut (*node_ptr).mask, mask);
            ptr::write(&mut (*node_ptr).capacity, capacity as u8);
            NodeRef { ptr: node_ptr }
        }
    }

    // Destroy the given node by first `dropping` all contained entries and then free the node's
    // memory.
    fn destroy(&mut self) {
        unsafe {
            for i in (0 .. self.entry_count()) {
                self.drop_entry(i)
            }

            // Let's use malloc and free for raw memory allocation so this library
            // build on 'stable':

            let align = mem::align_of::<AlignmentStruct<K, V, IS, H>>();
            let header_size = align_to(mem::size_of::<UnsafeNode<K, V, IS, H>>(), align);
            let node_size = header_size + (self.capacity as usize) * UnsafeNode::<K, V, IS, H>::node_entry_size();
            deallocate(mem::transmute(self), node_size, align);
        }
    }

    // Drops a single entry. Does not modify the entry_types or mask field of the node, just calls
    // the destructor of the entry at the given index.
    unsafe fn drop_entry(&mut self, index: usize) {
        // destroy the contained object, trick from Rc
        match self.get_entry_mut(index) {
            NodeEntryMutRef::Item(item_ref) => {
                let _ = ptr::read(item_ref as *mut IS as *const IS);
            }
            NodeEntryMutRef::Collision(item_ref) => {
                let _ = ptr::read(item_ref as *mut Arc<Vec<IS>> as *const Arc<Vec<IS>>);
            }
            NodeEntryMutRef::SubTree(item_ref) => {
                let _ = ptr::read(item_ref as *mut NodeRef<K, V, IS, H> as *const NodeRef<K, V, IS, H>);
            }
        }
    }
}

// impl UnsafeNode (continued)
impl<K, V, IS, H> UnsafeNode<K, V, IS, H>
    where K: Eq+Hash,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
    // Insert a new key-value pair into the tree. The existing tree is not modified and a new tree
    // is created. This new tree will share most nodes with the existing one.
    fn insert(&self,
              // The *remaining* hash value. For every level down the tree, this value is shifted
              // to the right by `BITS_PER_LEVEL`
              hash: u64,
              // The current level of the tree
              level: usize,
              // The key-value pair to be inserted
              new_kvp: IS,
              // The number of newly inserted items. Must be set to either 0 (if an existing item is
              // replaced) or 1 (if there was not item with the given key yet). Used to keep track
              // of the trees total item count
              insertion_count: &mut usize)
              // Reference to the new tree containing the inserted element
           -> NodeRef<K, V, IS, H> {

        debug_assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as usize;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            let new_node = self.copy_with_new_entry(local_key, NodeEntryOwned::Item(new_kvp));
            return new_node;
        }

        let index = get_index(self.mask, local_key);

        match self.get_entry(index) {
            NodeEntryRef::Item(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    self.copy_with_new_entry(local_key, NodeEntryOwned::Item(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = hash_of::<K, H>(&existing_key) >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. return a copy of this node with the single-item entry replaced by the new
                    // subtree entry
                    self.copy_with_new_entry(local_key, NodeEntryOwned::SubTree(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = vec!(new_kvp, existing_kvp_ref.clone());
                    self.copy_with_new_entry(local_key, NodeEntryOwned::Collision(Arc::new(items)))
                }
            }
            NodeEntryRef::Collision(items_arc) => {
                debug_assert!(level == LAST_LEVEL);
                let items = &*items_arc;
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = Vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.extend(items.iter().cloned());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = Vec::with_capacity(item_count);

                        if position > 0 {
                            new_items.extend(items.iter().take(position).cloned());
                        }

                        new_items.push(new_kvp);

                        if position < item_count - 1 {
                           new_items.extend(items.iter().skip(position + 1).cloned());
                        }

                        debug_assert!(new_items.len() == item_count);
                        new_items
                    }
                };

                self.copy_with_new_entry(local_key, NodeEntryOwned::Collision(Arc::new(new_items)))
            }
            NodeEntryRef::SubTree(sub_tree_ref) => {
                let new_sub_tree = sub_tree_ref.borrow().insert(hash >> BITS_PER_LEVEL,
                                                                level + 1,
                                                                new_kvp,
                                                                insertion_count);

                self.copy_with_new_entry(local_key, NodeEntryOwned::SubTree(new_sub_tree))
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
                           level: usize,
                           new_kvp: IS,
                           insertion_count: &mut usize)
                        -> Option<NodeRef<K, V, IS, H>> {

        debug_assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as usize;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            if self.entry_count() < self.capacity as usize {
                // If yes, then fill it with a single-item entry
                *insertion_count = 1;
                self.insert_entry_in_place(local_key, NodeEntryOwned::Item(new_kvp));
                return None;
            } else {
                // else fall back to copying
                return Some(self.insert(hash, level, new_kvp, insertion_count));
            }
        }

        let index = get_index(self.mask, local_key);

        // If there is no space left in this node but we would need it, again fall back to copying
        if self.entry_count() == self.capacity as usize &&
           self.get_entry_type_code(index) != SUBTREE_ENTRY {
            return Some(self.insert(hash, level, new_kvp, insertion_count));
        }

        let new_entry = match self.get_entry_mut(index) {
            NodeEntryMutRef::Item(existing_kvp_ref) => {
                let existing_key = existing_kvp_ref.key();

                if *existing_key == *new_kvp.key() {
                    *insertion_count = 0;
                    // Replace entry for the given key
                    Some(NodeEntryOwned::Item(new_kvp))
                } else if level != LAST_LEVEL {
                    *insertion_count = 1;
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level:

                    // 1. build the hashes for the level below
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = hash_of::<K, H>(existing_key) >> (BITS_PER_LEVEL * (level + 1));

                    // 2. create the sub tree, containing the two items
                    let new_sub_tree = UnsafeNode::new_with_entries(new_kvp,
                                                                    new_hash,
                                                                    existing_kvp_ref,
                                                                    existing_hash,
                                                                    level + 1);

                    // 3. replace the ItemEntryRef entry with the subtree entry
                    Some(NodeEntryOwned::SubTree(new_sub_tree))
                } else {
                    *insertion_count = 1;
                    // If we have already exhausted all bits from the hash value, put everything in
                    // collision node
                    let items = vec!(new_kvp, existing_kvp_ref.clone());
                    let collision_entry = NodeEntryOwned::Collision(Arc::new(items));
                    Some(collision_entry)
                }
            }
            NodeEntryMutRef::Collision(items) => {
                debug_assert!(level == LAST_LEVEL);
                let position = items.iter().position(|kvp2| *kvp2.key() == *new_kvp.key());

                let new_items = match position {
                    None => {
                        *insertion_count = 1;

                        let mut new_items = Vec::with_capacity(items.len() + 1);
                        new_items.push(new_kvp);
                        new_items.extend(items.iter().cloned());
                        new_items
                    }
                    Some(position) => {
                        *insertion_count = 0;

                        let item_count = items.len();
                        let mut new_items = Vec::with_capacity(item_count);

                        if position > 0 {
                            new_items.extend(items.iter().take(position).cloned());
                        }

                        new_items.push(new_kvp);

                        if position < item_count - 1 {
                           new_items.extend(items.iter().skip(position + 1).cloned());
                        }

                        debug_assert!(new_items.len() == item_count);
                        new_items
                    }
                };

                Some(NodeEntryOwned::Collision(Arc::new(new_items)))
            }
            NodeEntryMutRef::SubTree(subtree_mut_ref) => {
                match subtree_mut_ref.try_borrow_owned() {
                    BorrowedNodeRef::Shared(subtree) => {
                        Some(NodeEntryOwned::SubTree(subtree.insert(hash >> BITS_PER_LEVEL,
                                               level + 1,
                                               new_kvp,
                                               insertion_count)))
                    }
                    BorrowedNodeRef::Exclusive(subtree) => {
                        match subtree.try_insert_in_place(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          new_kvp.clone(),
                                                          insertion_count) {
                            Some(new_sub_tree) => Some(NodeEntryOwned::SubTree(new_sub_tree)),
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
              level: usize,
              key: &K,
              removal_count: &mut usize)
           -> RemovalResult<K, V, IS, H> {

        debug_assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as usize;

        if (self.mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return RemovalResult::NoChange;
        }

        let index = get_index(self.mask, local_key);

        match self.get_entry(index) {
            NodeEntryRef::Item(existing_kvp_ref) => {
                if *existing_kvp_ref.key() == *key {
                    *removal_count = 1;
                    self.collapse_kill_or_change(local_key, index)
                } else {
                    *removal_count = 0;
                    RemovalResult::NoChange
                }
            }
            NodeEntryRef::Collision(items_arc) => {
                debug_assert!(level == LAST_LEVEL);
                let items = &*items_arc;
                let position = items.iter().position(|kvp| *kvp.key() == *key);

                match position {
                    None => {
                        *removal_count = 0;
                        RemovalResult::NoChange
                    },
                    Some(position) => {
                        *removal_count = 1;
                        let item_count = items.len() - 1;

                        // The new entry can either still be a collision node, or it can be a simple
                        // single-item node if the hash collision has been resolved by the removal
                        let new_entry = if item_count > 1 {
                            let mut new_items = Vec::with_capacity(item_count);

                            if position > 0 {
                                new_items.extend(items.iter().take(position).cloned());
                            }
                            if position < item_count - 1 {
                                new_items.extend(items.iter().skip(position + 1).cloned());
                            }
                            debug_assert!(new_items.len() == item_count);

                            NodeEntryOwned::Collision(Arc::new(new_items))
                        } else {
                            debug_assert!(items.len() == 2);
                            debug_assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = items[index_of_remaining_item].clone();

                            NodeEntryOwned::Item(kvp)
                        };

                        let new_sub_tree = self.copy_with_new_entry(local_key, new_entry);
                        RemovalResult::ReplaceSubTree(new_sub_tree)
                    }
                }
            }
            NodeEntryRef::SubTree(sub_tree_ref) => {
                let result = sub_tree_ref.borrow().remove(hash >> BITS_PER_LEVEL,
                                                          level + 1,
                                                          key,
                                                          removal_count);
                match result {
                    RemovalResult::NoChange => RemovalResult::NoChange,
                    RemovalResult::ReplaceSubTree(x) => {
                        RemovalResult::ReplaceSubTree(
                            self.copy_with_new_entry(local_key,
                                                     NodeEntryOwned::SubTree(x)))
                    }
                    RemovalResult::CollapseSubTree(kvp) => {
                        if bit_count(self.mask) == 1 {
                            RemovalResult::CollapseSubTree(kvp)
                        } else {
                            RemovalResult::ReplaceSubTree(
                                self.copy_with_new_entry(local_key,
                                                         NodeEntryOwned::Item(kvp)))
                        }
                    },
                    RemovalResult::KillSubTree => {
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
                       level: usize,
                       key: &K,
                       removal_count: &mut usize)
                    -> RemovalResult<K, V, IS, H> {
        debug_assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as usize;
        let mask = self.mask;

        if (mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return RemovalResult::NoChange;
        }

        let index = get_index(mask, local_key);

        enum Action<K, V, IS, H> {
            CollapseKillOrChange,
            NoAction,
            ReplaceEntry(NodeEntryOwned<K, V, IS, H>)
        }

        let action: Action<K, V, IS, H> = match self.get_entry_mut(index) {
            NodeEntryMutRef::Item(existing_kvp_ref) => {
                if *existing_kvp_ref.key() == *key {
                    *removal_count = 1;
                    Action::CollapseKillOrChange
                } else {
                    *removal_count = 0;
                    Action::NoAction
                }
            }
            NodeEntryMutRef::Collision(items) => {
                debug_assert!(level == LAST_LEVEL);
                let position = items.iter().position(|kvp| *kvp.key() == *key);

                match position {
                    None => {
                        *removal_count = 0;
                        Action::NoAction
                    },
                    Some(position) => {
                        *removal_count = 1;
                        let item_count = items.len() - 1;

                        // The new entry can either still be a collision node, or it can be a simple
                        // single-item node if the hash collision has been resolved by the removal
                        let new_entry = if item_count > 1 {
                            let mut new_items = Vec::with_capacity(item_count);

                            if position > 0 {
                                new_items.extend(items.iter().take(position).cloned());
                            }
                            if position < item_count - 1 {
                                new_items.extend(items.iter().skip(position + 1).cloned());
                            }
                            debug_assert!(new_items.len() == item_count);

                            NodeEntryOwned::Collision(Arc::new(new_items))
                        } else {
                            debug_assert!(items.len() == 2);
                            debug_assert!(position == 0 || position == 1);
                            let index_of_remaining_item = 1 - position;
                            let kvp = (**items)[index_of_remaining_item].clone();

                            NodeEntryOwned::Item(kvp)
                        };

                        Action::ReplaceEntry(new_entry)
                    }
                }
            }
            NodeEntryMutRef::SubTree(sub_tree_ref) => {
                let result = match sub_tree_ref.try_borrow_owned() {
                    BorrowedNodeRef::Shared(node_ref) => node_ref.remove(hash >> BITS_PER_LEVEL,
                                                            level + 1,
                                                            key,
                                                            removal_count),
                    BorrowedNodeRef::Exclusive(node_ref) => node_ref.remove_in_place(hash >> BITS_PER_LEVEL,
                                                                    level + 1,
                                                                    key,
                                                                    removal_count)
                };

                match result {
                    RemovalResult::NoChange => Action::NoAction,
                    RemovalResult::ReplaceSubTree(x) => {
                        Action::ReplaceEntry(NodeEntryOwned::SubTree(x))
                    }
                    RemovalResult::CollapseSubTree(kvp) => {
                        if bit_count(mask) == 1 {
                            return RemovalResult::CollapseSubTree(kvp);
                        }

                        Action::ReplaceEntry(NodeEntryOwned::Item(kvp))
                    },
                    RemovalResult::KillSubTree => {
                        Action::CollapseKillOrChange
                    }
                }
            }
        };

        match action {
            Action::NoAction => RemovalResult::NoChange,
            Action::CollapseKillOrChange => self.collapse_kill_or_change_in_place(local_key, index),
            Action::ReplaceEntry(new_entry) => {
                self.insert_entry_in_place(local_key, new_entry);
                RemovalResult::NoChange
            }
        }
    }

    // Determines how the parent node should handle the removal of the entry at local_key from this
    // node.
    fn collapse_kill_or_change(&self,
                               local_key: usize,
                               entry_index: usize)
                            -> RemovalResult<K, V, IS, H> {
        let new_entry_count = bit_count(self.mask) - 1;

        if new_entry_count > 1 {
            RemovalResult::ReplaceSubTree(self.copy_without_entry(local_key))
        } else if new_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.get_entry(other_index) {
                NodeEntryRef::Item(kvp_ref) => {
                    RemovalResult::CollapseSubTree(kvp_ref.clone())
                }
                _ => RemovalResult::ReplaceSubTree(self.copy_without_entry(local_key))
            }
        } else {
            debug_assert!(new_entry_count == 0);
            RemovalResult::KillSubTree
        }
    }

    // Same as `collapse_kill_or_change()` but will do the modification in-place.
    fn collapse_kill_or_change_in_place(&mut self,
                                        local_key: usize,
                                        entry_index: usize)
                                     -> RemovalResult<K, V, IS, H> {
        let new_entry_count = self.entry_count() - 1;

        if new_entry_count > 1 {
            self.remove_entry_in_place(local_key);
            RemovalResult::NoChange
        } else if new_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.get_entry(other_index) {
                NodeEntryRef::Item(kvp_ref) => {
                    return RemovalResult::CollapseSubTree(kvp_ref.clone())
                }
                _ => { /* */ }
            }

            self.remove_entry_in_place(local_key);
            RemovalResult::NoChange
        } else {
            debug_assert!(new_entry_count == 0);
            RemovalResult::KillSubTree
        }
    }

    // Copies this node with a new entry at `local_key`. Might replace an old entry.
    fn copy_with_new_entry(&self,
                           local_key: usize,
                           new_entry: NodeEntryOwned<K, V, IS, H>)
                        -> NodeRef<K, V, IS, H> {
        let replace_old_entry = (self.mask & (1 << local_key)) != 0;
        let new_mask: MaskType = self.mask | (1 << local_key);
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

            debug_assert!(new_i == new_node.entry_count() as usize);
        }

        return new_node_ref;
    }

    // Inserts a new node entry in-place. Will take care of modifying node entry data, including the
    // node's mask and entry_types fields.
    fn insert_entry_in_place(&mut self,
                             local_key: usize,
                             new_entry: NodeEntryOwned<K, V, IS, H>) {
        let new_mask: MaskType = self.mask | (1 << local_key);
        let replace_old_entry = new_mask == self.mask;
        let index = get_index(new_mask, local_key);

        if replace_old_entry {
            // Destroy the replaced entry
            unsafe {
                self.drop_entry(index);
                self.init_entry(index, new_entry);
            }
        } else {
            debug_assert!(self.capacity as usize > self.entry_count());
            // make place for new entry:
            unsafe {
                if index < self.entry_count() {
                    let source: *const u8 = self.get_entry_ptr(index);
                    let dest: *mut u8 = mem::transmute(
                        source.offset(UnsafeNode::<K, V, IS, H>::node_entry_size() as isize));
                    let count = (self.entry_count() - index) *
                        UnsafeNode::<K, V, IS, H>::node_entry_size();
                    ptr::copy(source, dest, count);

                    let type_mask_up_to_index: EntryTypesType = ENTRY_TYPES_MAX << ((index + 1) * 2);
                    self.entry_types = ((self.entry_types << 2) & type_mask_up_to_index) |
                                       (self.entry_types & !type_mask_up_to_index);
                }
            }

            self.mask = new_mask;
            self.init_entry(index, new_entry);
        }
    }

    // Given that the current capacity is too small, returns how big the new node should be.
    fn expanded_capacity(&self) -> usize {
        if self.capacity == 0 {
            MIN_CAPACITY
        } else if self.capacity > 16 {
            32
        } else {
            ((self.capacity as usize) * 2)
        }
    }

    // Create a copy of this node which does not contain the entry at 'local_key'.
    fn copy_without_entry(&self, local_key: usize) -> NodeRef<K, V, IS, H> {
        debug_assert!((self.mask & (1 << local_key)) != 0);

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

            debug_assert!(new_i == bit_count(new_mask));
        }
        return new_node_ref;
    }

    // Same as `copy_without_entry()` but applies the modification in place.
    fn remove_entry_in_place(&mut self, local_key: usize) {
        debug_assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let index = get_index(self.mask, local_key);

        unsafe {
            self.drop_entry(index);

            if index < self.entry_count() - 1 {
                let source: *const u8 = self.get_entry_ptr(index + 1);
                let dest: *mut u8 = mem::transmute(
                    source.offset(-(UnsafeNode::<K, V, IS, H>::node_entry_size() as isize))
                    );
                let count = (self.entry_count() - (index + 1)) *
                    UnsafeNode::<K, V, IS, H>::node_entry_size();
                ptr::copy(source, dest, count);

                let type_mask_up_to_index: EntryTypesType = ENTRY_TYPES_MAX << ((index + 1) * 2);
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
                        level: usize)
                     -> NodeRef<K, V, IS, H> {
        debug_assert!(level <= LAST_LEVEL);

        let new_local_key = (new_hash & LEVEL_BIT_MASK) as usize;
        let existing_local_key = (existing_hash & LEVEL_BIT_MASK) as usize;

        if new_local_key != existing_local_key {
            let mask = (1 << new_local_key) | (1 << existing_local_key);
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();

                if new_local_key < existing_local_key {
                    new_node.init_entry(0, NodeEntryOwned::Item(new_kvp));
                    new_node.init_entry(1, NodeEntryOwned::Item(existing_kvp.clone()));
                } else {
                    new_node.init_entry(0, NodeEntryOwned::Item(existing_kvp.clone()));
                    new_node.init_entry(1, NodeEntryOwned::Item(new_kvp));
                };
            }
            new_node_ref
        } else if level == LAST_LEVEL {
            let mask = 1 << new_local_key;
            let mut new_node_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
            {
                let new_node = new_node_ref.borrow_mut();
                let items = vec!(new_kvp, existing_kvp.clone());
                new_node.init_entry(0, NodeEntryOwned::Collision(Arc::new(items)));
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
                new_node.init_entry(0, NodeEntryOwned::SubTree(sub_tree));
            }
            new_node_ref
        }
    }
}



//=-------------------------------------------------------------------------------------------------
// HamtMap
//=-------------------------------------------------------------------------------------------------
pub struct HamtMap<K, V, IS=ShareStore<K,V>, H=StdHasher> {
    root: NodeRef<K, V, IS, H>,
    element_count: usize,
}

// impl HamtMap
impl<K, V, IS, H> HamtMap<K, V, IS, H>
    where K: Eq+Hash,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
    pub fn new() -> HamtMap<K, V, IS, H> {

        static mut EMPTY: UnsafeNode<u128,
                                     u128,
                                     ::item_store::CopyStore<u128, u128>,
                                     StdHasher> = UnsafeNode {
            ref_count: AtomicUsize::new(0xFF),
            entry_types: 0,
            mask: 0,
            capacity: 0,
            __entries: [],
        };

        unsafe {
            // Yes, that's right, we are directly modifying the ref-count of
            // the static mut UnsafeNode. Because we can.
            EMPTY.ref_count.fetch_add(1, Ordering::SeqCst);

            HamtMap {
                root: NodeRef {
                    ptr: mem::transmute(&EMPTY)
                },
                element_count: 0
            }
        }
    }

    pub fn iter<'a>(&'a self) -> HamtMapIterator<'a, K, V, IS, H> {
        HamtMapIterator::new(self)
    }

    pub fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        // let mut hash = key.hash();
        let mut hash = hash_of::<K, H>(key);

        let mut level = 0;
        let mut current_node = self.root.borrow();

        loop {
            debug_assert!(level <= LAST_LEVEL);
            let local_key = (hash & LEVEL_BIT_MASK) as usize;

            if (current_node.mask & (1 << local_key)) == 0 {
                return None;
            }

            let index = get_index(current_node.mask, local_key);

            match current_node.get_entry(index) {
                NodeEntryRef::Item(kvp_ref) => return if *key == *kvp_ref.key() {
                    Some(kvp_ref.val())
                } else {
                    None
                },
                NodeEntryRef::Collision(items) => {
                    debug_assert!(level == LAST_LEVEL);
                    let found = items.iter().find(|&kvp| *key == *kvp.key());
                    return match found {
                        Some(kvp) => Some(kvp.val()),
                        None => None,
                    };
                }
                NodeEntryRef::SubTree(subtree_ref) => {
                    debug_assert!(level < LAST_LEVEL);
                    current_node = subtree_ref.borrow();
                    hash = hash >> BITS_PER_LEVEL;
                    level += 1;
                }
            };
        }
    }

    fn insert_internal(self, kvp: IS) -> (HamtMap<K, V, IS, H>, bool) {
        let HamtMap { mut root, element_count } = self;
        let hash = hash_of::<K, H>(kvp.key());
        let mut insertion_count = 0xdeadbeaf;

        // If we hold the only reference to the root node, then try to insert the KVP in-place
        let new_root = match root.try_borrow_owned() {
            BorrowedNodeRef::Exclusive(mutable) => mutable.try_insert_in_place(hash, 0, kvp, &mut insertion_count),
            BorrowedNodeRef::Shared(immutable) => Some(immutable.insert(hash, 0, kvp, &mut insertion_count))
        };

        // Make sure that insertion_count was set properly
        debug_assert!(insertion_count != 0xdeadbeaf);

        match new_root {
            Some(r) => (
                HamtMap {
                    root: r,
                    element_count: element_count + insertion_count
                },
                insertion_count != 0
            ),
            None => (
                HamtMap {
                    root: root,
                    element_count: element_count + insertion_count
                },
                insertion_count != 0
            )
        }
    }

    fn try_remove_in_place(self, key: &K) -> (HamtMap<K, V, IS, H>, bool) {
        let HamtMap { mut root, element_count } = self;
        let hash = hash_of::<K, H>(key);
        let mut removal_count = 0xdeadbeaf;

        let removal_result = match root.try_borrow_owned() {
            BorrowedNodeRef::Shared(node_ref) => node_ref.remove(hash, 0, key, &mut removal_count),
            BorrowedNodeRef::Exclusive(node_ref) => node_ref.remove_in_place(hash, 0, key, &mut removal_count)
        };
        debug_assert!(removal_count != 0xdeadbeaf);
        let new_element_count = element_count - removal_count;

        (match removal_result {
            RemovalResult::NoChange => HamtMap {
                root: root,
                element_count: new_element_count
            },
            RemovalResult::ReplaceSubTree(new_root) => HamtMap {
                root: new_root,
                element_count: new_element_count
            },
            RemovalResult::CollapseSubTree(kvp) => {
                debug_assert!(bit_count(root.borrow().mask) == 2);
                let local_key = (hash_of::<K, H>(kvp.key()) & LEVEL_BIT_MASK) as usize;

                let mask = 1 << local_key;
                let mut new_root_ref = UnsafeNode::alloc(mask, MIN_CAPACITY);
                {
                    let root = new_root_ref.borrow_mut();
                    root.init_entry(0, NodeEntryOwned::Item(kvp));
                }
                HamtMap {
                    root: new_root_ref,
                    element_count: new_element_count
                }
            }
            RemovalResult::KillSubTree => {
                debug_assert!(bit_count(root.borrow().mask) == 1);
                HamtMap::new()
            }
        }, removal_count != 0)
    }

    pub fn len(&self) -> usize {
        self.element_count
    }

    /// Inserts a key-value pair into the map. An existing value for a
    /// key is replaced by the new value. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    pub fn insert(self, key: K, value: V) -> (HamtMap<K, V, IS, H>, bool) {
        self.insert_internal(ItemStore::new(key, value))
    }

    /// Removes a key-value pair from the map. The first tuple element of the return value is the new
    /// map instance representing the map after the insertion. The second tuple element is true if
    /// the size of the map was changed by the operation and false otherwise.
    pub fn remove(self, key: &K) -> (HamtMap<K, V, IS, H>, bool) {
        self.try_remove_in_place(key)
    }


    /// Inserts a key-value pair into the map. Same as `insert()` but with a return type that's
    /// better suited to chaining multiple calls together.
    pub fn plus(self, key: K, val: V) -> HamtMap<K, V, IS, H> {
        self.insert(key, val).0
    }

    /// Removes a key-value pair from the map. Same as `remove()` but with a return type that's
    /// better suited to chaining multiple call together
    pub fn minus(self, key: &K) -> HamtMap<K, V, IS, H> {
        self.remove(key).0
    }
}

// Clone for HamtMap
impl<K, V, IS, H> Clone for HamtMap<K, V, IS, H> {
    fn clone(&self) -> HamtMap<K, V, IS, H> {
        HamtMap {
            root: self.root.clone(),
            element_count: self.element_count
        }
    }
}

// Default for HamtMap
impl<K, V, IS, H> Default for HamtMap<K, V, IS, H>
    where K: Eq+Hash,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
    fn default() -> HamtMap<K, V, IS, H> {
        HamtMap::new()
    }
}

impl<'a, K, V, IS, H> IntoIterator for &'a HamtMap<K, V, IS, H>
    where K: Eq+Hash+'a,
          V: 'a,
          IS: ItemStore<K, V>+'a,
          H: Hasher+Default+'a
{
    type Item = (&'a K, &'a V);
    type IntoIter = HamtMapIterator<'a, K, V, IS, H>;

    fn into_iter(self) -> HamtMapIterator<'a, K, V, IS, H>
    {
        self.iter()
    }
}

// Eq for HamtMap
impl<K, V, IS, H> PartialEq for HamtMap<K, V, IS, H>
    where K: Eq+Hash,
          V: PartialEq,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
    fn eq(&self, other: &HamtMap<K, V, IS, H>) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for (k, other_value) in other.iter() {
            match self.find(k) {
                Some(this_value) => {
                    if *this_value != *other_value {
                        return false;
                    }
                }
                None => {
                    return false;
                }
            }
        }

        true
    }

    fn ne(&self, other: &HamtMap<K, V, IS, H>) -> bool {
        !(*self == *other)
    }
}


// Eq for HamtMap
impl<K, V, IS, H> Eq for HamtMap<K, V, IS, H>
    where K: Eq+Hash,
          V: Eq,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
}


// FromIterator
impl<K, V, IS, H> ::std::iter::FromIterator<(K, V)> for HamtMap<K, V, IS, H>
    where K: Eq+Hash,
          IS: ItemStore<K, V>,
          H: Hasher+Default
{
    fn from_iter<T>(iterator: T) -> Self where T: IntoIterator<Item=(K, V)> {

        let mut map = HamtMap::new();

        for (k, v) in iterator {
            map = map.plus(k, v);
        }

        map
    }
}


//=-------------------------------------------------------------------------------------------------
// HamtMapIterator
//=-------------------------------------------------------------------------------------------------

#[derive(Copy)]
enum IterNodeRef<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    RegularNode(&'a UnsafeNode<K, V, IS, H>),
    CollisionEntry(&'a Vec<IS>)
}

impl<'a, K, V, IS, H> Clone for IterNodeRef<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    fn clone(&self) -> Self {
        match *self {
            IterNodeRef::RegularNode(x) => IterNodeRef::RegularNode(x),
            IterNodeRef::CollisionEntry(x) => IterNodeRef::CollisionEntry(x)
        }
    }
}

pub struct HamtMapIterator<'a, K, V, IS, H>
    where K: 'a,
          V: 'a,
          IS: 'a,
          H: 'a
{
    node_stack: [(IterNodeRef<'a, K, V, IS, H>, isize); LAST_LEVEL + 2],
    stack_size: usize,
    len: usize,
}

impl<'a, K, V, IS, H>
HamtMapIterator<'a, K, V, IS, H>
    where K: Eq,
          IS: ItemStore<K, V>,
          H: Hasher
{
    fn new(map: &'a HamtMap<K, V, IS, H>) -> HamtMapIterator<'a, K, V, IS, H> {
        let mut iterator = HamtMapIterator {
            node_stack: unsafe{ mem::zeroed() },
            stack_size: 1,
            len: map.element_count,
        };

        iterator.node_stack[0] = (IterNodeRef::RegularNode(map.root.borrow()), -1);
        iterator
    }
}

impl<'a, K, V, IS, H>
Iterator for HamtMapIterator<'a, K, V, IS, H>
    where K: Eq,
          IS: ItemStore<K, V>,
          H: 'a + Hasher
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.stack_size == 0 {
            return None;
        }

        let (current_node, index) = self.node_stack[self.stack_size - 1].clone();
        let next_index: usize = (index + 1) as usize;

        match current_node {
            IterNodeRef::RegularNode(node_ref) => {
                if next_index == node_ref.entry_count() {
                    self.stack_size -= 1;
                    return self.next();
                } else {
                    let (_, ref mut stack_index) = self.node_stack[self.stack_size - 1];
                    *stack_index = next_index as isize;
                }

                match node_ref.get_entry(next_index) {
                    NodeEntryRef::Item(item_ref) => {
                        return Some((item_ref.key(), item_ref.val()));
                    }
                    NodeEntryRef::Collision(items_arc) => {
                        let items = &**items_arc;
                        self.node_stack[self.stack_size] = (IterNodeRef::CollisionEntry(items), 0);
                        self.stack_size += 1;
                        let item = &items[0];
                        return Some((item.key(), item.val()));
                    },
                    NodeEntryRef::SubTree(subtree_ref) => {
                        self.node_stack[self.stack_size] = (IterNodeRef::RegularNode(subtree_ref.borrow()), -1);
                        self.stack_size += 1;
                        return self.next();
                    }
                };
            }
            IterNodeRef::CollisionEntry(items_ref) => {
                if next_index == items_ref.len() {
                    self.stack_size -= 1;
                    return self.next();
                }

                let item = &items_ref[next_index];
                return Some((item.key(), item.val()));
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

//=-------------------------------------------------------------------------------------------------
// Utility functions
//=------------------------------------------------------------------------------------------------
#[inline]
fn get_index(mask: MaskType, index: usize) -> usize {
    debug_assert!((mask & (1 << index)) != 0);

    let bits_set_up_to_index = (1 << index) - 1;
    let masked = mask & bits_set_up_to_index;

    bit_count(masked)
}

#[inline]
fn bit_count(x: MaskType) -> usize {
    x.count_ones() as usize
}

#[inline]
fn align_to(size: usize, align: usize) -> usize {
    debug_assert!(align != 0 && bit_count(align as MaskType) == 1);
    (size + align - 1) & !(align - 1)
}

#[inline]
fn hash_of<T: Hash, H: Hasher + Default>(value: &T) -> u64 {
    let mut h: H = Default::default();
    value.hash(&mut h);
    h.finish()
}

#[inline(always)]
pub unsafe fn allocate(size: usize, _align: usize) -> *mut u8 {
    libc::malloc(size as libc::size_t) as *mut u8
}

#[inline(always)]
pub unsafe fn deallocate(ptr: *mut u8, _old_size: usize, _align: usize) {
    libc::free(ptr as *mut libc::c_void)
}

#[cfg(test)]
mod tests {
    use super::get_index;
    use super::HamtMap;
    use testing::Test;
    use std::collections::HashMap;

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
        let mut map: HamtMap<u64, u64, CopyStore> = HamtMap::new();
        let count = 1000usize;

        for i in (0u64 .. count as u64) {
            map = map.plus(i, i);
        }

        let it = map.iter();
        assert_eq!((count, Some(count)), it.size_hint());

        let reference: HashMap<u64, u64> = it.map(|(x, y)| (*x, *y)).collect();

        assert_eq!(count, reference.len());

        for i in (0u64 .. count as u64) {
            assert_eq!(reference.get(&i), Some(&i));
        }
    }

    #[test]
    fn test_insert_copy() {
        Test::test_insert(HamtMap::<u64, u64, CopyStore>::new());
    }

    #[test]
    fn test_insert_ascending_copy() {
        Test::test_insert_ascending(HamtMap::<u64, u64, CopyStore>::new());
    }

    #[test]
    fn test_insert_descending_copy() {
        Test::test_insert_descending(HamtMap::<u64, u64, CopyStore>::new());
    }

    #[test]
    fn test_insert_overwrite_copy() {
        Test::test_insert_overwrite(HamtMap::<u64, u64, CopyStore>::new());
    }

    #[test]
    fn test_remove_copy() {
        Test::test_remove(HamtMap::<u64, u64, CopyStore>::new());
    }

    #[test]
    fn test_default_copy() {
        Test::test_default::<CopyStore>();
    }

    #[test]
    fn test_eq_empty_copy() {
        Test::test_eq_empty::<CopyStore>();
    }

    #[test]
    fn test_eq_random_copy() {
        Test::test_eq_random::<CopyStore>();
    }

    #[test]
    fn stress_test_copy() {
        Test::random_insert_remove_stress_test(HamtMap::<u64, u64, CopyStore>::new());
    }



//=-------------------------------------------------------------------------------------------------
// Test HamtMap<ShareStore>
//=-------------------------------------------------------------------------------------------------

    #[test]
    fn test_iterator_share() {
        let mut map: HamtMap<u64, u64, ShareStore> = HamtMap::new();
        let count = 1000usize;

        for i in (0u64 .. count as u64) {
            map = map.plus(i, i);
        }

        let it = map.iter();
        assert_eq!((count, Some(count)), it.size_hint());

        let test: HashMap<u64, u64> = it.map(|(x, y)| (*x, *y)).collect();

        assert_eq!(count, test.len());

        for i in (0u64 .. count as u64) {
            assert_eq!(test.get(&i), Some(&i));
        }
    }

    #[test]
    fn test_insert_share() {
        Test::test_insert(HamtMap::<u64, u64, ShareStore>::new());
    }

    #[test]
    fn test_insert_ascending_share() {
        Test::test_insert_ascending(HamtMap::<u64, u64, ShareStore>::new());
    }

    #[test]
    fn test_insert_descending_share() {
        Test::test_insert_descending(HamtMap::<u64, u64, ShareStore>::new());
    }

    #[test]
    fn test_insert_overwrite_share() {
        Test::test_insert_overwrite(HamtMap::<u64, u64, ShareStore>::new());
    }

    #[test]
    fn test_remove_share() {
        Test::test_remove(HamtMap::<u64, u64, ShareStore>::new());
    }

    #[test]
    fn stress_test_share() {
        Test::random_insert_remove_stress_test(HamtMap::<u64, u64, ShareStore>::new());
    }
}
