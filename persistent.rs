
use std::cast;
use std::vec;
use std::unstable::intrinsics;

static LAST_LEVEL: uint = (64 / 5) - 1;
static BITS_PER_LEVEL: uint = 5;
static LEVEL_BIT_MASK: u64 = 0b11111;

enum NodeEntry<K, V> {
    Collision(@~[(@K, @V)]),
    SingleItem(@K, @V),
    SubTree(@Node<K, V>)
}

impl<K, V> Clone for NodeEntry<K, V> {
    fn clone(&self) -> NodeEntry<K, V> {
        match *self {
            Collision(items) => Collision(items),
            SingleItem(k, v) => SingleItem(k, v),
            SubTree(sub) => SubTree(sub),
        }
    }
}

struct Node<K, V> {
    mask: u32,
    entries: ~[NodeEntry<K, V>],
}

impl<K:Hash + Eq + 'static, V: 'static> Node<K, V> {
    fn insert(@self, hash: u64, level: uint, key: K, val: V) -> @Node<K, V> {
        assert!(level <= LAST_LEVEL);

        let local_key = (hash & LEVEL_BIT_MASK) as uint;

        if (self.mask & (1 << local_key)) == 0 {
            return self.copy_with_new_entry(local_key, SingleItem(@key, @val));
        }

        let index = get_index(self.mask, local_key);

        match self.entries[index] {
            SingleItem(k, v) => {
                if *k == key {
                    // Replace entry for the given key
                    self.copy_with_new_entry(local_key, SingleItem(@key, @val))
                } else if level != LAST_LEVEL {
                    // There already is an entry with different key but same hash value, so push
                    // everything down one level
                    let new_hash = hash >> BITS_PER_LEVEL;
                    let existing_hash = (*k).hash() >> (BITS_PER_LEVEL * (level + 1));

                    let new_sub_tree = Node::new_with_entries(@key, @val, new_hash, k, v, existing_hash, level + 1);
                    self.copy_with_new_entry(local_key, SubTree(new_sub_tree))
                } else {
                    let collision_entry = Collision(@~[(@key, @val), (k, v)]);
                    self.copy_with_new_entry(local_key, collision_entry)
                }
            }
            Collision(@ref items) => {
                assert!(level == LAST_LEVEL);
                let position = items.iter().position(|&(@ref k, _)| *k == key);

                let new_items = match position {
                    None => {
                        let mut new_items = vec::with_capacity(items.len() + 1);
                        new_items.push((@key, @val));
                        new_items.push_all(items.as_slice());
                        new_items
                    }
                    Some(position) => {
                        let mut new_items = items.to_owned();
                        new_items[position] = (@key, @val);
                        new_items
                    }
                };

                self.copy_with_new_entry(local_key, Collision(@new_items))
            }
            SubTree(sub_tree) => {
                let new_sub_tree = sub_tree.insert(hash >> BITS_PER_LEVEL, level + 1, key, val);
                self.copy_with_new_entry(local_key, SubTree(new_sub_tree))
            }
        }
    }

    fn copy_with_new_entry(&self,
                           local_key: uint,
                           new_entry: NodeEntry<K, V>)
                        -> @Node<K, V> {
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

        return @Node {
            mask: new_mask,
            entries: new_entry_list
        };
    }

    fn new_with_entries(new_key: @K,
                        new_val: @V,
                        new_hash: u64,
                        existing_key: @K,
                        existing_val: @V,
                        existing_hash: u64,
                        level: uint)
                     -> @Node<K, V> {
        assert!(level <= LAST_LEVEL);

        let new_local_key = new_hash & LEVEL_BIT_MASK;
        let existing_local_key = existing_hash & LEVEL_BIT_MASK;

        if new_local_key != existing_local_key {
            let mask = (1 << new_local_key) | (1 << existing_local_key);
            let entries = if new_local_key < existing_local_key {
                ~[SingleItem(new_key, new_val), SingleItem(existing_key, existing_val)]
            } else {
                ~[SingleItem(existing_key, existing_val), SingleItem(new_key, new_val)]
            };

            @Node {
                mask: mask,
                entries: entries,
            }
        } else if level == LAST_LEVEL {
            @Node {
                mask: 1 << new_local_key,
                entries: ~[Collision(@~[(new_key, new_val), (existing_key, existing_val)])],
            }
        } else {
            // recurse further
            let sub_tree = Node::new_with_entries(new_key,
                                                  new_val,
                                                  new_hash >> BITS_PER_LEVEL,
                                                  existing_key,
                                                  existing_val,
                                                  existing_hash >> BITS_PER_LEVEL,
                                                  level + 1);
            @Node {
                mask: 1 << new_local_key,
                entries: ~[SubTree(sub_tree)]
            }
        }
    }
}

struct HamtMap<K, V> {
    root: @Node<K, V>
}

impl<K:Hash + Eq + 'static, V: 'static> HamtMap<K, V> {

    fn new() -> HamtMap<K, V> {
        HamtMap {
            root: @Node {
                mask: 0,
                entries: ~[],
            }
        }
    }

    fn find(&self, key: &K) -> Option<@V> {
        let mut hash = key.hash();

        let mut level = 0;
        let mut current_node = self.root;

        while level <= LAST_LEVEL {
            let local_key = (hash & LEVEL_BIT_MASK) as uint;

            if (current_node.mask & (1 << local_key)) == 0 {
                return None;
            }

            let index = get_index(current_node.mask, local_key);

            match current_node.entries[index] {
                SingleItem(@ref k, val) => return if *key == *k {
                    Some(val)
                } else {
                    None
                },
                Collision(ref items) => {
                    assert!(level == LAST_LEVEL);
                    let found = items.iter().find(|kvp| **(kvp.first_ref()) == *key);
                    return match found {
                        Some(&(_, val)) => Some(val),
                        None => None,
                    };
                }
                SubTree(sub) => {
                    assert!(level < LAST_LEVEL);
                    current_node = sub;
                    hash = hash >> BITS_PER_LEVEL;
                    level += 1;
                }
            };
        }

        unreachable!();
    }

    fn insert(&self, key: K, val: V) -> HamtMap<K, V> {
        let hash = key.hash();
        let new_root = self.root.insert(hash, 0, key, val);
        HamtMap { root: new_root }
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

        let map01 = map00.insert(1, 2);
        let map10 = map00.insert(2, 4);
        let map11 = map01.insert(2, 4);

        assert_eq!(map00.find(&1), None);
        assert_eq!(map00.find(&2), None);

        assert_eq!(map01.find(&1), Some(@2));
        assert_eq!(map01.find(&2), None);

        assert_eq!(map11.find(&1), Some(@2));
        assert_eq!(map11.find(&2), Some(@4));

        assert_eq!(map10.find(&1), None);
        assert_eq!(map10.find(&2), Some(@4));
    }

    #[test]
    fn test_insert_overwrite() {
        let empty = HamtMap::new();
        let mapA = empty.insert(1, 2);
        let mapB = mapA.insert(1, 4);

        assert_eq!(empty.find(&1), None);
        assert_eq!(mapA.find(&1), Some(@2));
        assert_eq!(mapB.find(&1), Some(@4));
    }

    #[test]
    fn test_random() {
        let mut values: HashSet<u64> = HashSet::new();
        let mut rng = rand::rng();

        for _ in range(0, 500000) {
            values.insert(rand::Rand::rand(&mut rng));
        }

        let mut map = HamtMap::new();

        for &x in values.iter() {
            map = map.insert(x, x);
        }

        for &x in values.iter() {
            assert_eq!(map.find(&x), Some(@x));
        }
    }
}

