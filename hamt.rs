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
use std::vec;
use std::unstable::intrinsics;
use extra::arc::Arc;
use persistent::PersistentMap;

static LAST_LEVEL: uint = (64 / 5) - 1;
static BITS_PER_LEVEL: uint = 5;
static LEVEL_BIT_MASK: u64 = 0b11111;

enum NodeEntry<K, V> {
    Collision(Arc<~[Arc<(K, V)>]>),
    SingleItem(Arc<(K, V)>),
    SubTree(Arc<Node<K, V>>)
}

impl<K:Hash + Eq + Send + Freeze, V: Send + Freeze> Clone for NodeEntry<K, V> {
    fn clone(&self) -> NodeEntry<K, V> {
        match *self {
            Collision(ref items) => Collision(items.clone()),
            SingleItem(ref kvp) => SingleItem(kvp.clone()),
            SubTree(ref sub) => SubTree(sub.clone()),
        }
    }
}

struct Node<K, V> {
    priv mask: u32,
    priv entries: ~[NodeEntry<K, V>],
}

enum RemovalResult<K, V> {
    // Don't do anything
    NoChange,
    // Replace the sub-tree entry with another sub-tree entry pointing to the given node
    ReplaceSubTree(Arc<Node<K, V>>),
    // Collapse the sub-tree into a singe-item entry
    CollapseSubTree(Arc<(K, V)>),
    // Completely remove the entry
    KillSubTree
}

impl<K:Hash + Eq + Send + Freeze, V: Send + Freeze> Node<K, V> {
    fn insert(&self,
              hash: u64,
              level: uint,
              new_kvp: Arc<(K, V)>,
              insertion_count: &mut uint)
           -> Arc<Node<K, V>> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        // See if the slot is free
        if (self.mask & (1 << local_key)) == 0 {
            // If yes, then fill it with a single-item entry
            *insertion_count = 1;
            return self.copy_with_new_entry(local_key, SingleItem(new_kvp));
        }

        let index = get_index(self.mask, local_key);

        match self.entries[index] {
            SingleItem(ref existing_entry) => {
                let existing_key = get_key(existing_entry);

                if *existing_key == *get_key(&new_kvp) {
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
                    let new_sub_tree = Node::new_with_entries(new_kvp,
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
                let position = items.iter().position(|kvp2| *get_key(kvp2) == *get_key(&new_kvp));

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
                let new_sub_tree = sub_tree.get().insert(hash >> BITS_PER_LEVEL,
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
           -> RemovalResult<K, V> {

        assert!(level <= LAST_LEVEL);
        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if (self.mask & (1 << local_key)) == 0 {
            *removal_count = 0;
            return NoChange;
        }

        let index = get_index(self.mask, local_key);

        match self.entries[index] {
            SingleItem(ref existing_kvp) => {
                if *get_key(existing_kvp) == *key {
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
                let position = items.iter().position(|kvp| *get_key(kvp) == *key);

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
                let result = sub_tree.get().remove(hash >> BITS_PER_LEVEL,
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
    fn collapse_kill_or_change(&self, local_key: uint, entry_index: uint) -> RemovalResult<K, V> {
        let next_entry_count = bit_count(self.mask) - 1;

        if next_entry_count > 1 {
            ReplaceSubTree(self.copy_without_entry(local_key))
        } else if next_entry_count == 1 {
            let other_index = 1 - entry_index;

            match self.entries[other_index] {
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
                           new_entry: NodeEntry<K, V>)
                        -> Arc<Node<K, V>> {
        let replace_old_entry = (self.mask & (1 << local_key)) != 0;
        let new_mask = self.mask | (1 << local_key);
        let mut new_entry_list = vec::with_capacity(bit_count(new_mask));
        let index = get_index(new_mask, local_key);

        let mut i = 0;

        // Copy up to index
        while i < index {
            new_entry_list.push(self.entries[i].clone());
            i += 1;
        }

        // Add new entry
        new_entry_list.push(new_entry);

        if replace_old_entry {
            // Skip the replaced value
            i += 1;
        }

        // Copy the rest
        while i < self.entries.len() {
            new_entry_list.push(self.entries[i].clone());
            i += 1;
        }

        assert!(new_entry_list.len() == bit_count(new_mask));

        return Arc::new(Node {
            mask: new_mask,
            entries: new_entry_list
        });
    }

    fn copy_without_entry(&self, local_key: uint) -> Arc<Node<K, V>> {
        assert!((self.mask & (1 << local_key)) != 0);

        let new_mask = self.mask & !(1 << local_key);
        let mut new_entry_list = vec::with_capacity(bit_count(new_mask));
        let index = get_index(self.mask, local_key);

        let mut i = 0;

        // Copy up to index
        while i < index {
            new_entry_list.push(self.entries[i].clone());
            i += 1;
        }

        i += 1;

        // Copy the rest
        while i < self.entries.len() {
            new_entry_list.push(self.entries[i].clone());
            i += 1;
        }

        assert!(new_entry_list.len() == bit_count(new_mask));

        return Arc::new(Node {
            mask: new_mask,
            entries: new_entry_list
        });
    }

    fn new_with_entries(new_kvp: Arc<(K,V)>,
                        new_hash: u64,
                        existing_kvp: &Arc<(K, V)>,
                        existing_hash: u64,
                        level: uint)
                     -> Arc<Node<K, V>> {
        assert!(level <= LAST_LEVEL);

        let new_local_key = new_hash & LEVEL_BIT_MASK;
        let existing_local_key = existing_hash & LEVEL_BIT_MASK;

        if new_local_key != existing_local_key {
            let mask = (1 << new_local_key) | (1 << existing_local_key);
            let entries = if new_local_key < existing_local_key {
                ~[SingleItem(new_kvp), SingleItem(existing_kvp.clone())]
            } else {
                ~[SingleItem(existing_kvp.clone()), SingleItem(new_kvp)]
            };

            Arc::new(Node {
                mask: mask,
                entries: entries,
            })
        } else if level == LAST_LEVEL {
            Arc::new(Node {
                mask: 1 << new_local_key,
                entries: ~[Collision(Arc::new(~[new_kvp, existing_kvp.clone()]))],
            })
        } else {
            // recurse further
            let sub_tree = Node::new_with_entries(new_kvp,
                                                  new_hash >> BITS_PER_LEVEL,
                                                  existing_kvp,
                                                  existing_hash >> BITS_PER_LEVEL,
                                                  level + 1);
            Arc::new(Node {
                mask: 1 << new_local_key,
                entries: ~[SubTree(sub_tree)]
            })
        }
    }
}

struct HamtMap<K, V> {
    priv root: Arc<Node<K, V>>,
    priv element_count: uint,
}

// Clone
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze> Clone for HamtMap<K, V> {
    fn clone(&self) -> HamtMap<K, V> {
        HamtMap { root: self.root.clone(), element_count: self.element_count }
    }
}

// Container
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze> Container for HamtMap<K, V> {
    fn len(&self) -> uint {
        self.element_count
    }
}

// Map
impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze> Map<K, V> for HamtMap<K, V> {

    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        let mut hash = key.hash();

        let mut level = 0;
        let mut current_node = &self.root;

        loop {
            assert!(level <= LAST_LEVEL);
            let local_key = (hash & LEVEL_BIT_MASK) as uint;

            if (current_node.get().mask & (1 << local_key)) == 0 {
                return None;
            }

            let index = get_index(current_node.get().mask, local_key);

            match current_node.get().entries[index] {
                SingleItem(ref kvp) => return if *key == *get_key(kvp) {
                    Some(kvp.get().second_ref())
                } else {
                    None
                },
                Collision(ref items) => {
                    assert!(level == LAST_LEVEL);
                    let found = items.get().iter().find(|&kvp| *key == *get_key(kvp));
                    return match found {
                        Some(kvp) => Some(kvp.get().second_ref()),
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
}

impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze> PersistentMap<K, V> for HamtMap<K, V> {

    fn insert(&self, key: K, val: V) -> (HamtMap<K, V>, bool) {
        let hash = key.hash();
        let mut new_entry_count = 0xdeadbeaf;
        let new_root = self.root.get().insert(hash,
                                              0,
                                              Arc::new((key, val)),
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

    fn remove(&self, key: &K) -> (HamtMap<K, V>, bool) {
        let hash = key.hash();
        let mut removal_count = 0xdeadbeaf;
        let removal_result = self.root.get().remove(hash, 0, key, &mut removal_count);
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
                assert!(bit_count(self.root.get().mask) == 2);
                let local_key = (get_key(&kvp).hash() & LEVEL_BIT_MASK) as uint;

                HamtMap {
                    root: Arc::new(Node {
                        mask: 1 << local_key,
                        entries: ~[SingleItem(kvp)]
                    }),
                    element_count: new_element_count
                }
            }
            KillSubTree => {
                assert!(bit_count(self.root.get().mask) == 1);
                HamtMap::new()
            }
        }, removal_count != 0)
    }
}

impl<K: Hash+Eq+Send+Freeze, V: Send+Freeze> HamtMap<K, V> {

    fn new() -> HamtMap<K, V> {
        HamtMap {
            root: Arc::new(Node {
                mask: 0,
                entries: ~[],
            }),
            element_count: 0
        }
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
fn get_key<'a, K: Hash+Eq+Send+Freeze, V: Send+Freeze>(arc: &'a Arc<(K,V)>) -> &'a K {
    arc.get().first_ref()
}


#[inline]
fn bit_count(x: u32) -> uint {
    unsafe {
        intrinsics::ctpop32(cast::transmute(x)) as uint
    }
}


// TESTS

macro_rules! assert_find(
    ($map:ident, $key:expr, None) => (
        assert!($map.find(&$key).is_none());
    );
    ($map:ident, $key:expr, $val:expr) => (
        match $map.find(&$key) {
            Some(&value) => {
                assert_eq!(value, $val);
            }
            _ => fail!()
        };
    );
)

#[cfg(test)]
mod tests {
    use super::get_index;
    use super::HamtMap;
    use std::hashmap::HashSet;
    use std::rand;
    use std::iter::range;

    #[test]
    fn test_get_index() {
        assert_eq!(get_index(0b00000000000000000000000000000001, 0) , 0);
        assert_eq!(get_index(0b00000000000000000000000000000010, 1) , 0);
        assert_eq!(get_index(0b00000000000000000000000000000100, 2) , 0);
        assert_eq!(get_index(0b10000000000000000000000000000000, 31) , 0);

        assert_eq!(get_index(0b00000000000000000000000000101010, 1) , 0);
        assert_eq!(get_index(0b00000000000000000000000000101010, 3) , 1);
        assert_eq!(get_index(0b00000000000000000000000000101010, 5) , 2);
    }

    #[test]
    fn test_insert() {
        let map00 = HamtMap::new();

        let (map01, new_entry01) = map00.insert(1, 2);
        let (map10, new_entry10) = map00.insert(2, 4);
        let (map11, new_entry11) = map01.insert(2, 4);

        assert_find!(map00, 1, None);
        assert_find!(map00, 2, None);

        assert_find!(map01, 1, 2);
        assert_find!(map01, 2, None);

        assert_find!(map11, 1, 2);
        assert_find!(map11, 2, 4);

        assert_find!(map10, 1, None);
        assert_find!(map10, 2, 4);

        assert_eq!(new_entry01, true);
        assert_eq!(new_entry10, true);
        assert_eq!(new_entry11, true);

        assert_eq!(map00.len(), 0);
        assert_eq!(map01.len(), 1);
        assert_eq!(map10.len(), 1);
        assert_eq!(map11.len(), 2);
    }

    #[test]
    fn test_insert_overwrite() {
        let empty = HamtMap::new();
        let (mapA, new_entryA) = empty.insert(1, 2);
        let (mapB, new_entryB) = mapA.insert(1, 4);
        let (mapC, new_entryC) = mapB.insert(1, 6);

        assert_find!(empty, 1, None);
        assert_find!(mapA, 1, 2);
        assert_find!(mapB, 1, 4);
        assert_find!(mapC, 1, 6);

        assert_eq!(new_entryA, true);
        assert_eq!(new_entryB, false);
        assert_eq!(new_entryC, false);

        assert_eq!(empty.len(), 0);
        assert_eq!(mapA.len(), 1);
        assert_eq!(mapB.len(), 1);
        assert_eq!(mapC.len(), 1);
    }

    #[test]
    fn test_remove() {
        let (map00, _) = (HamtMap::new()
            .insert(1, 2)).first()
            .insert(2, 4);

        let (map01, _) = map00.remove(&1);
        let (map10, _) = map00.remove(&2);
        let (map11, _) = map01.remove(&2);

        assert_find!(map00, 1, 2);
        assert_find!(map00, 2, 4);

        assert_find!(map01, 1, None);
        assert_find!(map01, 2, 4);

        assert_find!(map11, 1, None);
        assert_find!(map11, 2, None);

        assert_find!(map10, 1, 2);
        assert_find!(map10, 2, None);

        assert_eq!(map00.len(), 2);
        assert_eq!(map01.len(), 1);
        assert_eq!(map10.len(), 1);
        assert_eq!(map11.len(), 0);
    }

    #[test]
    fn test_random() {
        let mut values: HashSet<u64> = HashSet::new();
        let mut rng = rand::rng();

        for _ in range(0, 20000) {
            values.insert(rand::Rand::rand(&mut rng));
        }

        let mut map = HamtMap::new();

        for &x in values.iter() {
            let (map1, _) = map.insert(x, x);
            map = map1;
        }

        for &x in values.iter() {
            assert_find!(map, x, x);
        }

        for (i, x) in values.iter().enumerate() {
            if i % 2 == 0 {
                let (map1, _) = map.remove(x);
                map = map1;
            }
        }

        for (i, &x) in values.iter().enumerate() {
            if i % 2 != 0 {
                assert_find!(map, x, x);
            } else {
                assert_find!(map, x, None);
            }
        }
    }
}
