// Copyright (c) 2014 Michael Woerister
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

use sync::Arc;
use persistent::PersistentMap;
use item_store::{ItemStore, CopyStore, ShareStore};

#[deriving(Clone, Eq)]
enum Color {
    NegativeBlack = 0,
    Red = 1,
    Black = 2,
    DoubleBlack = 3
}

impl Color {
    fn inc(&self) -> Color {
        match *self {
            NegativeBlack => Red,
            Red => Black,
            Black => DoubleBlack,
            DoubleBlack => fail!("Can't inc DoubleBlack")
        }
    }

    fn dec(&self) -> Color {
        match *self {
            NegativeBlack => fail!("Can't dec NegativeBlack"),
            Red => NegativeBlack,
            Black => Red,
            DoubleBlack => Black
        }
    }
}

struct NodeData<K, V, IS> {
    left: NodeRef<K, V, IS>,
    item: IS,
    right: NodeRef<K, V, IS>,
}

struct NodeRef<K, V, IS> {
    col: Color,
    data: Option<Arc<NodeData<K, V, IS>>>
}

impl<K, V, IS> Clone for NodeRef<K, V, IS> {
    fn clone(&self) -> NodeRef<K, V, IS> {
        NodeRef {
            col: self.col,
            data: self.data.clone()
        }
    }
}

fn new_node<K: Ord+Clone+Send+Freeze,
            V: Clone+Send+Freeze,
            IS: ItemStore<K, V>>(
                color: Color,
                left: NodeRef<K, V, IS>,
                item: IS,
                right: NodeRef<K, V, IS>)
             -> NodeRef<K, V, IS> {
    let node = NodeRef {
        col: color,
        data: Some(
            Arc::new(
                NodeData {
                    left: left,
                    item: item,
                    right: right
                }
            )
        )
    };
    assert!(!node.is_leaf());
    return node;
}

fn new_leaf<K: Ord+Clone+Send+Freeze,
            V: Clone+Send+Freeze,
            IS: ItemStore<K, V>>(
                color: Color)
             -> NodeRef<K, V, IS> {
    assert!(color == Black || color == DoubleBlack);
    let leaf = NodeRef { col: color, data: None };
    assert!(leaf.is_leaf());
    return leaf;
}

impl<K: Ord+Clone+Send+Freeze, V: Clone+Send+Freeze, IS: ItemStore<K, V>> NodeRef<K, V, IS> {

    fn is_leaf(&self) -> bool {
        self.data.is_none()
    }

    fn get_data<'a>(&'a self) -> &'a NodeData<K, V, IS> {
        match self.data {
            Some(ref data_ref) => data_ref.get(),
            None => unreachable!()
        }
    }

    fn redden(self) -> NodeRef<K, V, IS> {
        assert!(!self.is_leaf());
        NodeRef {
            col: Red,
            data: self.data
        }
    }

    fn blacken(self) -> NodeRef<K, V, IS> {
        NodeRef {
            col: Black,
            data: self.data
        }
    }

    fn inc(mut self) -> NodeRef<K, V, IS> {
        self.col = self.col.inc();
        self
    }

    fn dec(mut self) -> NodeRef<K, V, IS> {
        self.col = self.col.dec();
        self
    }

    fn find<'a>(&'a self, search_key: &K) -> Option<&'a V> {
        match self.data {
            Some(ref data_ref) => {
                let data_ref = data_ref.get();

                if *search_key < *data_ref.item.key() {
                    data_ref.left.find(search_key)
                } else if *search_key > *data_ref.item.key() {
                    data_ref.right.find(search_key)
                } else {
                    Some(data_ref.item.val())
                }
            }
            None => None
        }
    }

    // Calculates the max black nodes on path:
    // fn count_black_height(&self, combine: |u64, u64| -> u64) -> u64 {
    //     assert!(self.col == Red || self.col == Black);

    //     match self.data {
    //         Some(ref data_ref) => {
    //             let data_ref = data_ref.get();
    //             let this = if self.col == Black { 1 } else { 0 };
    //             let sub = combine(data_ref.left.count_black_height(|a, b| combine(a, b)),
    //                               data_ref.right.count_black_height(|a, b| combine(a, b)));
    //             this + sub
    //         }
    //         None => { 1 }
    //     }
    // }

    // Does this tree contain a red child of red?
    // fn no_red_red(&self) -> bool {
    //     assert!(self.col == Red || self.col == Black);
    //     if !self.is_leaf() {
    //         let (l, r) = self.children();
    //         assert!(l.col == Red || l.col == Black);
    //         assert!(r.col == Red || r.col == Black);

    //         if self.col == Black {
    //             return l.no_red_red() && r.no_red_red();
    //         }

    //         if self.col == Red && l.col == Black && r.col == Black {
    //             return l.no_red_red() && r.no_red_red();
    //         }

    //         return false;
    //     } else {
    //         return true;
    //     }
    // }

    // Is this tree black-balanced?
    // fn black_balanced(&self) -> bool {
    //     self.count_black_height(::std::num::max) == self.count_black_height(::std::num::min)
    // }

    // Returns the maxium (key . value) pair:
    fn find_max_kvp<'a>(&'a self) -> &'a IS {
        assert!(!self.is_leaf());
        let node = self.get_data();
        if node.right.is_leaf() {
            &node.item
        } else {
            node.right.find_max_kvp()
        }
    }

    fn modify_at(&self, kvp: IS, insertion_count: &mut uint) -> NodeRef<K, V, IS> {
        self.modify_at_rec(kvp, insertion_count).blacken()
    }

    fn modify_at_rec(&self, kvp: IS, insertion_count: &mut uint) -> NodeRef<K, V, IS> {
        if self.is_leaf() {
            assert!(self.col == Black);
            *insertion_count = 1;
            new_node(Red,
                     new_leaf(Black),
                     kvp,
                     new_leaf(Black))
        } else {
            let node_data = self.get_data();
            let node_color = self.col;

            if *kvp.key() < *node_data.item.key() {
                new_node(node_color,
                         node_data.left.modify_at_rec(kvp, insertion_count),
                         node_data.item.clone(),
                         node_data.right.clone())
                .balance()
            } else if *kvp.key() > *node_data.item.key() {
                new_node(node_color,
                         node_data.left.clone(),
                         node_data.item.clone(),
                         node_data.right.modify_at_rec(kvp, insertion_count))
                .balance()
            } else {
                *insertion_count = 0;
                new_node(node_color,
                         node_data.left.clone(),
                         kvp,
                         node_data.right.clone())
            }
        }
    }

    // WAT!
    fn balance(self) -> NodeRef<K, V, IS> {
        assert!(!self.is_leaf());
        {
            let root_data = self.get_data();
            let left_child = &root_data.left;
            let right_child = &root_data.right;

            if self.col == Black || self.col == DoubleBlack {
                let result_col = self.col.dec();


                if left_child.col == Red {
                    assert!(!left_child.is_leaf());
                    let left_child_data = left_child.get_data();
                    let left_grand_child = &left_child_data.left;
                    let right_grand_child = &left_child_data.right;

                    if left_grand_child.col == Red {
                        assert!(!left_grand_child.is_leaf());
                        let left_grand_child_data = left_grand_child.get_data();
                        return new_node(result_col,
                                        new_node(Black,
                                                 left_grand_child_data.left.clone(),
                                                 left_grand_child_data.item.clone(),
                                                 left_grand_child_data.right.clone()),
                                        left_child.get_data().item.clone(),
                                        new_node(Black,
                                                 right_grand_child.clone(),
                                                 root_data.item.clone(),
                                                 right_child.clone())
                        );
                    }

                    if right_grand_child.col == Red {
                        assert!(!right_grand_child.is_leaf());
                        let right_grand_child_data = right_grand_child.get_data();
                        return new_node(result_col,
                                        new_node(Black,
                                                 left_grand_child.clone(),
                                                 left_child.get_data().item.clone(),
                                                 right_grand_child_data.left.clone()),
                                        right_grand_child_data.item.clone(),
                                        new_node(Black,
                                                 right_grand_child_data.right.clone(),
                                                 root_data.item.clone(),
                                                 right_child.clone()));
                    }
                }

                if right_child.col == Red {
                    assert!(!right_child.is_leaf());
                    let right_child_data = right_child.get_data();
                    let left_grand_child = &right_child_data.left;
                    let right_grand_child = &right_child_data.right;

                    if left_grand_child.col == Red {
                        assert!(!left_grand_child.is_leaf());
                        let left_grand_child_data = left_grand_child.get_data();
                        return new_node(result_col,
                                        new_node(Black,
                                                 left_child.clone(),
                                                 root_data.item.clone(),
                                                 left_grand_child_data.left.clone()),
                                        left_grand_child_data.item.clone(),
                                        new_node(Black,
                                                 left_grand_child_data.right.clone(),
                                                 right_child_data.item.clone(),
                                                 right_grand_child.clone()));
                    }

                    if right_grand_child.col == Red {
                        assert!(!right_grand_child.is_leaf());
                        let right_grand_child_data = right_grand_child.get_data();

                        return new_node(result_col,
                                        new_node(Black,
                                                 left_child.clone(),
                                                 root_data.item.clone(),
                                                 left_grand_child.clone()),
                                        right_child_data.item.clone(),
                                        new_node(Black,
                                                 right_grand_child_data.left.clone(),
                                                 right_grand_child_data.item.clone(),
                                                 right_grand_child_data.right.clone()));
                    }
                }
            }

            if self.col == DoubleBlack {
                if right_child.col == NegativeBlack {
                    assert!(!right_child.is_leaf());
                    let right_child_data = right_child.get_data();
                    let left_grand_child = &right_child_data.left;
                    let right_grand_child = &right_child_data.right;

                    if !left_grand_child.is_leaf() &&
                       left_grand_child.col == Black &&
                       right_grand_child.col == Black {
                        let left_grand_child_data = left_grand_child.get_data();
                        return new_node(Black,
                                        new_node(Black,
                                                 left_child.clone(),
                                                 root_data.item.clone(),
                                                 left_grand_child_data.left.clone()),
                                        left_grand_child_data.item.clone(),
                                        new_node(Black,
                                                 left_grand_child_data.right.clone(),
                                                 right_child_data.item.clone(),
                                                 right_grand_child.clone().redden()).balance()
                        );
                    }
                }

                if left_child.col == NegativeBlack {
                    assert!(!left_child.is_leaf());
                    let left_child_data = left_child.get_data();
                    let left_grand_child = &left_child_data.left;
                    let right_grand_child = &left_child_data.right;

                    if left_grand_child.col == Black &&
                       !right_grand_child.is_leaf() &&
                       right_grand_child.col == Black {
                        let right_grand_child_data = right_grand_child.get_data();
                        return new_node(Black,
                                        new_node(Black,
                                                 left_grand_child.clone().redden(),
                                                 left_child_data.item.clone(),
                                                 right_grand_child_data.left.clone()).balance(),
                                        right_grand_child_data.item.clone(),
                                        new_node(Black,
                                                 right_grand_child_data.right.clone(),
                                                 root_data.item.clone(),
                                                 right_child.clone()));
                    }
                }
            }
        }

        return self;
    }

    // Deletes a key from this map
    fn delete(&self, search_key: &K, removal_count: &mut uint) -> NodeRef<K, V, IS> {
        // Finds the node to be removed
        fn del<K: Ord+Clone+Send+Freeze, V: Clone+Send+Freeze, IS: ItemStore<K, V>>(
            node: &NodeRef<K, V, IS>,
            search_key: &K,
            removal_count: &mut uint)
         -> NodeRef<K, V, IS> {
            if !node.is_leaf() {
                let node_data = node.get_data();
                let node_key = node_data.item.key();

                if *search_key < *node_key {
                    bubble(node.col,
                           del(&node_data.left, search_key, removal_count),
                           node_data.item.clone(),
                           node_data.right.clone())
                } else if *search_key > *node_key {
                    bubble(node.col,
                           node_data.left.clone(),
                           node_data.item.clone(),
                           del(&node_data.right, search_key, removal_count))
                } else {
                    *removal_count = 1;
                    remove(node)
                }
            } else {
                *removal_count = 0;
                new_leaf(Black)
            }
        }

        // Removes this node. might leave behind a double-black node:
        fn remove<K: Ord+Clone+Send+Freeze, V: Clone+Send+Freeze, IS: ItemStore<K, V>>(
            node: &NodeRef<K, V, IS>) -> NodeRef<K, V, IS> {
            assert!(!node.is_leaf());

            let node_data = node.get_data();
            let left = &node_data.left;
            let right = &node_data.right;

            if left.is_leaf() && right.is_leaf() {
                return if node.col == Red {
                    new_leaf(Black)
                } else {
                    assert!(node.col == Black);
                    new_leaf(DoubleBlack)
                };
            }

            if node.col == Red {
                if right.is_leaf() {
                    return left.clone();
                }

                if left.is_leaf() {
                    return right.clone();
                }
            }

            if node.col == Black {
                if left.col == Red && right.is_leaf() {
                    let left_child_data = left.get_data();
                    return new_node(Black,
                                    left_child_data.left.clone(),
                                    left_child_data.item.clone(),
                                    left_child_data.right.clone());
                }

                if left.is_leaf() && right.col == Red {
                    let right_child_data = right.get_data();
                    return new_node(Black,
                                    right_child_data.left.clone(),
                                    right_child_data.item.clone(),
                                    right_child_data.right.clone());
                }

                if left.is_leaf() && right.col == Black {
                    return right.clone().inc();
                }

                if left.col == Black && right.is_leaf() {
                    return left.clone().inc();
                }
            }

            if !left.is_leaf() && !right.is_leaf() {
                let kvp = left.find_max_kvp();
                return bubble(node.col,
                              remove_max(left),
                              kvp.clone(),
                              right.clone());
            }

            unreachable!();
        }

        // Kills a double-black, or moves it to the top:
        fn bubble<K: Ord+Clone+Send+Freeze,
                  V: Clone+Send+Freeze,
                  IS: ItemStore<K, V>>(
                    color: Color,
                    l: NodeRef<K, V, IS>,
                    kvp: IS,
                    r: NodeRef<K, V, IS>)
                 -> NodeRef<K, V, IS> {
            if l.col == DoubleBlack || r.col == DoubleBlack {
                new_node(color.inc(), l.dec(), kvp, r.dec()).balance()
            } else {
                new_node(color, l, kvp, r)
            }
        }

        // Removes the max node:
        fn remove_max<K: Ord+Clone+Send+Freeze,
                      V: Clone+Send+Freeze,
                      IS: ItemStore<K, V>>(
                        node: &NodeRef<K, V, IS>)
                     -> NodeRef<K, V, IS> {
            assert!(!node.is_leaf());
            let node_data = node.get_data();
            if node_data.right.is_leaf() {
                remove(node)
            } else {
                bubble(node.col,
                       node_data.left.clone(),
                       node_data.item.clone(),
                       remove_max(&node_data.right))
            }
        }

        // Delete the key, and color the new root black
        del(self, search_key, removal_count).blacken()
    }
}

#[deriving(Clone)]
struct RedBlackTree<K, V, IS> {
    root: NodeRef<K, V, IS>,
    len: uint,
}

impl<K: Ord+Clone+Send+Freeze, V: Clone+Send+Freeze, IS: ItemStore<K, V>> RedBlackTree<K, V, IS> {
    pub fn new() -> RedBlackTree<K, V, IS> {
        RedBlackTree {
            root: new_leaf(Black),
            len: 0,
        }
    }

    pub fn find<'a>(&'a self, search_key: &K) -> Option<&'a V> {
        self.root.find(search_key)
    }

    pub fn insert(self, kvp: IS) -> (RedBlackTree<K, V, IS>, bool) {
        let mut insertion_count = 0xdeadbeaf;
        let new_root = self.root.modify_at(kvp, &mut insertion_count);
        assert!(insertion_count != 0xdeadbeaf);
        (RedBlackTree { root: new_root, len: self.len + insertion_count }, insertion_count != 0)
    }

    pub fn remove(self, key: &K) -> (RedBlackTree<K, V, IS>, bool) {
        let mut removal_count = 0xdeadbeaf;
        let new_root = self.root.delete(key, &mut removal_count);
        assert!(removal_count != 0xdeadbeaf);
        (RedBlackTree { root: new_root, len: self.len - removal_count }, removal_count != 0)
    }

    // fn balanced(&self) -> bool {
    //     self.root.black_balanced()
    // }

    // fn no_red_red(&self) -> bool {
    //     self.root.no_red_red()
    // }
}

impl<K: Hash+Eq+Send+Freeze+Ord+Clone, V: Send+Freeze+Clone> PersistentMap<K, V> for RedBlackTree<K, V, CopyStore<K, V>> {
    #[inline]
    fn insert(self, key: K, value: V) -> (RedBlackTree<K, V, CopyStore<K, V>>, bool) {
        self.insert(CopyStore { key: key, val: value })
    }

    #[inline]
    fn remove(self, key: &K) -> (RedBlackTree<K, V, CopyStore<K, V>>, bool) {
        self.remove(key)
    }
}

impl<K: Hash+Eq+Send+Freeze+Ord+Clone, V: Send+Freeze+Clone> PersistentMap<K, V> for RedBlackTree<K, V, ShareStore<K, V>> {
    #[inline]
    fn insert(self, key: K, value: V) -> (RedBlackTree<K, V, ShareStore<K, V>>, bool) {
        self.insert(ShareStore::new(key, value))
    }

    #[inline]
    fn remove(self, key: &K) -> (RedBlackTree<K, V, ShareStore<K, V>>, bool) {
        self.remove(key)
    }
}

impl<K: Hash+Eq+Send+Freeze+Ord+Clone, V: Send+Freeze+Clone, IS: ItemStore<K, V>> Map<K, V> for RedBlackTree<K, V, IS> {
    #[inline]
    fn find<'a>(&'a self, key: &K) -> Option<&'a V> {
        self.find(key)
    }
}

impl<K: Hash+Eq+Send+Freeze+Ord+Clone, V: Send+Freeze+Clone, IS: ItemStore<K, V>> Container for RedBlackTree<K, V, IS> {
    #[inline]
    fn len(&self) -> uint {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::RedBlackTree;
    use test::Test;
    use extra::test::BenchHarness;
    use item_store::{CopyStore, ShareStore};

    #[test]
    fn test_insert_copy() { Test::test_insert(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new()); }

    #[test]
    fn test_insert_overwrite_copy() { Test::test_insert_overwrite(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new()); }

    #[test]
    fn test_remove_copy() { Test::test_remove(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new()); }

    #[test]
    fn test_random_copy() { Test::test_random(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new()); }

    #[bench]
    fn bench_insert_copy_10(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_copy_100(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_copy_1000(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_copy_50000(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_copy_10(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_copy_100(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_copy_1000(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_copy_50000(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 50000, bh);
    }

    #[bench]
    fn bench_remove_copy_10(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_remove_copy_100(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_remove_copy_1000(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_remove_copy_50000(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, CopyStore<uint, uint>>::new(), 50000, bh);
    }

    #[test]
    fn test_insert_shared() { Test::test_insert(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new()); }

    #[test]
    fn test_insert_overwrite_shared() { Test::test_insert_overwrite(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new()); }

    #[test]
    fn test_remove_shared() { Test::test_remove(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new()); }

    #[test]
    fn test_random_shared() { Test::test_random(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new()); }

    #[bench]
    fn bench_insert_shared_10(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_insert_shared_100(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_insert_shared_1000(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_insert_shared_50000(bh: &mut BenchHarness) {
        Test::bench_insert(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 50000, bh);
    }

    #[bench]
    fn bench_find_shared_10(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_find_shared_100(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_find_shared_1000(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_find_shared_50000(bh: &mut BenchHarness) {
        Test::bench_find(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 50000, bh);
    }

    #[bench]
    fn bench_remove_shared_10(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 10, bh);
    }

    #[bench]
    fn bench_remove_shared_100(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 100, bh);
    }

    #[bench]
    fn bench_remove_shared_1000(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 1000, bh);
    }

    #[bench]
    fn bench_remove_shared_50000(bh: &mut BenchHarness) {
        Test::bench_remove(RedBlackTree::<uint, uint, ShareStore<uint, uint>>::new(), 50000, bh);
    }
}