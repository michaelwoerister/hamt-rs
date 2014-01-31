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
#[feature(macro_rules)];

use std::rc::Rc;

#[deriving(Clone, Eq)]
enum Color {
    NegativeBlack,
    Red,
    Black,
    DoubleBlack
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

struct NodeData {
    left: NodeRef,
    key: int,
    val: int,
    right: NodeRef,
}

impl Clone for NodeData {
    fn clone(&self) -> NodeData {
        NodeData {
            left: self.left.clone(),
            key: self.key.clone(),
            val: self.val.clone(),
            right: self.right.clone(),
        }
    }
}

struct NodeRef {
    col: Color,
    data: Option<Rc<NodeData>>
}

impl Clone for NodeRef {
    fn clone(&self) -> NodeRef {
        NodeRef {
            col: self.col,
            data: self.data.clone()
        }
    }
}

fn new_node(color: Color, left: NodeRef, key: int, val: int, right: NodeRef) -> NodeRef {
    let node = NodeRef {
        col: color,
        data: Some(
            Rc::new(
                NodeData {
                    left: left,
                    key: key,
                    val: val,
                    right: right
                }
            )
        )
    };
    assert!(!node.is_leaf());
    return node;
}

fn new_leaf(color: Color) -> NodeRef {
    assert!(color == Black || color == DoubleBlack);
    let leaf = NodeRef { col: color, data: None };
    assert!(leaf.is_leaf());
    return leaf;
}

impl NodeRef {

    fn is_leaf(&self) -> bool {
        self.data.is_none()
    }

    fn children(&self) -> (NodeRef, NodeRef) {
        match self.data {
            Some(ref data_ref) => {
                let data_ref = data_ref.borrow();
                (data_ref.left.clone(), data_ref.right.clone())
            }
            None => unreachable!()
        }
    }

    fn get_data<'a>(&'a self) -> &'a NodeData {
        match self.data {
            Some(ref data_ref) => data_ref.borrow(),
            None => unreachable!()
        }
    }

    fn children_and_key(&self) -> (NodeRef, int, NodeRef) {
        match self.data {
            Some(ref data_ref) => {
                let data_ref = data_ref.borrow();
                (data_ref.left.clone(), data_ref.key, data_ref.right.clone())
            }
            None => unreachable!()
        }
    }

    // (define/match (redden node)
    //   [(T cmp _ l k v r)   (T cmp 'R l k v r)]
    //   [(L _)               (error "Can't redden leaf.")])
    fn redden(self) -> NodeRef {
        assert!(!self.is_leaf());
        NodeRef {
            col: Red,
            data: self.data
        }
    }

    // (define/match (blacken node)
    // [(T cmp _ l k v r)  (T cmp 'B l k v r)]
    // [(BBL cmp)          (L cmp)]
    // [(L _)              node])
    fn blacken(self) -> NodeRef {
        NodeRef {
            col: Black,
            data: self.data
        }
    }

    fn inc(mut self) -> NodeRef {
        self.col = self.col.inc();
        self
    }

    fn dec(mut self) -> NodeRef {
        self.col = self.col.dec();
        self
    }

    fn find<'a>(&'a self, search_key: &int) -> Option<&'a int> {
        match self.data {
            Some(ref data_ref) => {
                let data_ref = data_ref.borrow();

                if *search_key < data_ref.key {
                    data_ref.left.find(search_key)
                } else if *search_key > data_ref.key {
                    data_ref.right.find(search_key)
                } else {
                    Some(&data_ref.val)
                }
            }
            None => None
        }
    }
}

// WAT!
fn balance_node(parent: NodeRef) -> NodeRef {
    assert!(!parent.is_leaf());

    if parent.col == Black || parent.col == DoubleBlack {
        let result_col = parent.col.dec();
        let node_data = parent.get_data();
        let (left_child, right_child) = parent.children();

        if left_child.col == Red {
            assert!(!left_child.is_leaf());
            // (T! (or 'B 'BB) (R (R a xk xv b) yk yv c) zk zv d)
            // (T! (or 'B 'BB) (R a xk xv (R b yk yv c)) zk zv d)
            let (left_grand_child, right_grand_child) = left_child.children();

            if left_grand_child.col == Red {
                assert!(!left_grand_child.is_leaf());
                // (T! (or 'B 'BB) (R (R a xk xv b) yk yv c) zk zv d)
                let (a, b) = left_grand_child.children();
                let xk = left_grand_child.get_data().key;
                let xv = left_grand_child.get_data().val;
                let yk = left_child.get_data().key;
                let yv = left_child.get_data().val;
                let c = right_grand_child;
                let zk = node_data.key;
                let zv = node_data.val;
                let d = right_child;
                return create_case1(result_col, a, b, c, d, xk, xv, yk, yv, zk, zv);
            }

            if right_grand_child.col == Red {
                assert!(!right_grand_child.is_leaf());
                // (T! (or 'B 'BB) (R a xk xv (R b yk yv c)) zk zv d)
                let a = left_grand_child;
                let xk = left_child.get_data().key;
                let xv = left_child.get_data().val;
                let (b, c) = right_grand_child.children();
                let yk = right_grand_child.get_data().key;
                let yv = right_grand_child.get_data().val;
                let zk = node_data.key;
                let zv = node_data.val;
                let d = right_child;
                return create_case1(result_col, a, b, c, d, xk, xv, yk, yv, zk, zv);
            }
        }

        if right_child.col == Red {
            assert!(!right_child.is_leaf());
            // (T! (or 'B 'BB) a xk xv (R (R b yk yv c) zk zv d))
            // (T! (or 'B 'BB) a xk xv (R b yk yv (R c zk zv d))))
            let a = left_child;
            let xk = node_data.key;
            let xv = node_data.val;
            let (left_grand_child, right_grand_child) = right_child.children();

            if left_grand_child.col == Red {
                assert!(!left_grand_child.is_leaf());
                // (T! (or 'B 'BB) a xk xv (R (R b yk yv c) zk zv d))
                let (b, c) = left_grand_child.children();
                let yk = left_grand_child.get_data().key;
                let yv = left_grand_child.get_data().val;
                let zk = right_child.get_data().key;
                let zv = right_child.get_data().val;
                let d = right_grand_child;
                return create_case1(result_col, a, b, c, d, xk, xv, yk, yv, zk, zv);
            }

            if right_grand_child.col == Red {
                assert!(!right_grand_child.is_leaf());
                // (T! (or 'B 'BB) a xk xv (R b yk yv (R c zk zv d))))
                let b = left_grand_child;
                let yk = right_child.get_data().key;
                let yv = right_child.get_data().val;
                let (c, d) = right_grand_child.children();
                let zk = right_grand_child.get_data().key;
                let zv = right_grand_child.get_data().val;
                return create_case1(result_col, a, b, c, d, xk, xv, yk, yv, zk, zv);
            }
        }
    }

    if parent.col == DoubleBlack {
        // (BB a xk xv (-B (B b yk yv c) zk zv (and d (B))))
        let (a, right_child) = parent.children();
        let xk = parent.get_data().key;
        let xv = parent.get_data().val;

        if right_child.col == NegativeBlack {
            assert!(!right_child.is_leaf());
            let (left_grand_child, right_grand_child) = right_child.children();

            if !left_grand_child.is_leaf() &&
               left_grand_child.col == Black &&
               right_grand_child.col == Black {
                let (b, c) = left_grand_child.children();
                let yk = left_grand_child.get_data().key;
                let yv = left_grand_child.get_data().val;
                let zk = right_child.get_data().key;
                let zv = right_child.get_data().val;
                let d = right_grand_child;

                // (T cmp 'B (T cmp 'B a xk xv b) yk yv (balance cmp 'B c zk zv (redden d)))
                return new_node(
                    Black,
                    new_node(Black, a, xk, xv, b),
                    yk,
                    yv,
                    balance_node(new_node(Black, c, zk, zv, d.redden()))
                );
            }
        }
    }

    if parent.col == DoubleBlack {
        // (BB (-B (and a (B)) xk xv (B b yk yv c)) zk zv d)
        let (left_child, d) = parent.children();

        if left_child.col == NegativeBlack {
            assert!(!left_child.is_leaf());
            let (left_grand_child, right_grand_child) = left_child.children();

            if left_grand_child.col == Black &&
               !right_grand_child.is_leaf() &&
               right_grand_child.col == Black {
                let a = left_grand_child;
                let xk = left_child.get_data().key;
                let xv = left_child.get_data().val;
                let (b, c) = right_grand_child.children();
                let yk = right_grand_child.get_data().key;
                let yv = right_grand_child.get_data().val;
                let zk = parent.get_data().key;
                let zv = parent.get_data().val;

                // (T cmp 'B (balance cmp 'B (redden a) xk xv b) yk yv (T cmp 'B c zk zv d))]
                return new_node(
                    Black,
                    balance_node(new_node(Black, a.redden(), xk, xv, b)),
                    yk,
                    yv,
                    new_node(Black, c, zk, zv, d)
                );
            }
        }
    }

    return parent;

    fn create_case1(color: Color,
                    a: NodeRef,
                    b: NodeRef,
                    c: NodeRef,
                    d: NodeRef,
                    xk: int,
                    xv: int,
                    yk: int,
                    yv: int,
                    zk: int,
                    zv: int,) -> NodeRef {
        // (T cmp (black-1 (T-color node)) (T cmp 'B a xk xv b) yk yv (T cmp 'B c zk zv d))
        return new_node(color, new_node(Black, a, xk, xv, b), yk, yv, new_node(Black, c, zk, zv, d));
    }
}

fn modify_at(node: &NodeRef, key: int, val: int) -> NodeRef {

    fn internal_modify_at(node: &NodeRef, key: int, val: int) -> NodeRef {
        if node.is_leaf() {
            assert!(node.col == Black);
            // (T cmp 'R node key (f key #f) node)]))
            new_node(Red, (*node).clone(), key, val, (*node).clone())
        } else {
            // matches: (T cmp c l k v r)
            let k = node.get_data().key;
            let v = node.get_data().val;
            let c = node.col;
            let (l, r) = node.children();

            if key < k {
                // (balance cmp c (internal-modify-at l key f) k v r)
                balance_node(new_node(c, internal_modify_at(&l, key, val), k, v, r))
            } else if key > k {
                // (balance cmp c l k v (internal-modify-at r key f))])
                balance_node(new_node(c, l, k, v, internal_modify_at(&r, key, val)))
            } else {
                // (T cmp c l k (f k v) r)
                new_node(c, l, k, val, r)
            }
        }
    }

    //(blacken (internal-modify-at node key f)))
    internal_modify_at(node, key, val).blacken()
}

// ; Deletes a key from this map:
fn delete(node: &NodeRef, search_key: &int) -> NodeRef {
    // Finds the node to be removed:
    fn del(node: NodeRef, search_key: &int) -> NodeRef {
        if !node.is_leaf() {
            let c = node.col;
            let k = node.get_data().key;
            let v = node.get_data().val;
            let (l, r) = node.children();

            if *search_key < k {
                bubble(c, del(l, search_key), k, v, r)
            } else if *search_key > k {
                bubble(c, l, k, v, del(r, search_key))
            } else {
                remove(node)
            }
        } else {
            node
        }
    }

    // Removes this node; it might
    // leave behind a double-black node:
    fn remove(node: NodeRef) -> NodeRef {
        assert!(!node.is_leaf());
        // Leaves are easiest to kill:
        let (left, right) = node.children();
        if left.is_leaf() && right.is_leaf() {
            return if node.col == Red {
                new_leaf(Black)
            } else {
                assert!(node.col == Black);
                new_leaf(DoubleBlack)
            };
        }

        // Killing a node with one child;
        // parent or child is red:
        //    [(or (R child (L!))
        //         (R (L!) child))
        // ; =>
        // child]
        if node.col == Red {
            if right.is_leaf() {
                return left;
            }

            if left.is_leaf() {
                return right;
            }
        }

        if node.col == Black {
            // [(or (B (R l k v r) (L!))
            if left.col == Red && right.is_leaf() {
                let NodeData { left: l, key: k, val: v, right: r } = (*left.get_data()).clone();
                // (T cmp 'B l k v r)]
                return new_node(Black, l, k, v, r);
            }

            //      (B (L!) (R l k v r)))
            if left.is_leaf() && right.col == Red {
                let NodeData { left: l, key: k, val: v, right: r } = (*right.get_data()).clone();
                // (T cmp 'B l k v r)]
                return new_node(Black, l, k, v, r);
            }

            // Killing a black node with one black child:
            // [(or (B (L!) (and child (B)))
            //      (B (and child (B)) (L!)))
            if left.is_leaf() && right.col == Black {
                // (black+1 child)
                let child = right.clone().inc();
                return child;
            }

            if left.col == Black && right.is_leaf() {
                // (black+1 child)
                let child = left.clone().inc();
                return child;
            }
        }

        if !left.is_leaf() && !right.is_leaf() {
            // ((cons k v) (sorted-map-max l))
            let (k, v) = sorted_map_max(&left);
            // (l*         (remove-max l))
            let new_left = remove_max(left);
            // (bubble c l* k v r)
            return bubble(node.col, new_left, k, v, right);
        }

        unreachable!();
    }

    // Kills a double-black, or moves it to the top:
    // (define (bubble c l k v r)
    fn bubble(color: Color, l: NodeRef, k: int, v: int, r: NodeRef) -> NodeRef {
        if l.col == DoubleBlack || r.col == DoubleBlack {
            // (or (double-black? l) (double-black? r))
            // (balance cmp (black+1 c) (black-1 l) k v (black-1 r))
            balance_node(new_node(color.inc(), l.dec(), k, v, r.dec()))
        } else {
            // [else (T cmp c l k v r)]
            new_node(color, l, k, v, r)
        }
    }

    // Removes the max node:
    fn remove_max(node: NodeRef) -> NodeRef {
        assert!(!node.is_leaf());
        let (left, key, right) = node.children_and_key();
        if right.is_leaf() {
            // [(T!   l     (L!))  (remove node)]
            remove(node)
        } else {
            //[(T! c l k v r   )  (bubble c l k v (remove-max r))])
            bubble(node.col, left, key, node.get_data().val, remove_max(right))
        }
    }

    // Delete the key, and color the new root black:
    // (blacken (del node)))
    del(node.clone(), search_key).blacken()
}

// Returns the maxium (key . value) pair:
fn sorted_map_max(node: &NodeRef) -> (int, int) {
    assert!(!node.is_leaf());
    let data = node.get_data();
    // let (_, key, right) = node.children_and_key();

    if data.right.is_leaf() {
        (data.key, data.val)
    } else {
        sorted_map_max(&data.right)
    }
}

#[deriving(Clone)]
struct PersistentRBT {
    root: NodeRef
}

impl PersistentRBT {
    pub fn new() -> PersistentRBT {
        PersistentRBT { root: new_leaf(Black) }
    }

    pub fn find<'a>(&'a self, search_key: &int) -> Option<&'a int> {
        self.root.find(search_key)
    }

    pub fn insert(self, key: int, value: int) -> (PersistentRBT, bool) {
        (PersistentRBT { root: modify_at(&self.root, key, value) }, false)
    }

    pub fn remove(self, key: &int) -> (PersistentRBT, bool) {
        (PersistentRBT { root: delete(&self.root, key) }, false)
    }

    fn balanced(&self) -> bool {
        black_balanced(&self.root)
    }

    fn no_red_red(&self) -> bool {
        no_red_red(&self.root)
    }
}

// Is this tree black-balanced?
fn black_balanced(node: &NodeRef) -> bool {
    // Calculates the max black nodes on path:
    fn max_black_height(node: &NodeRef) -> u64 {
        assert!(node.col == Red || node.col == Black);

        match node.data {
            // [(T! c l r)
            Some(ref data_ref) => {
                let data_ref = data_ref.borrow();
                // (+ (if (eq? c 'B) 1 0) (max (max-black-height l)
                //                  (max-black-height r)))
                let this = if node.col == Black { 1 } else { 0 };
                let sub = std::num::max(max_black_height(&data_ref.left), max_black_height(&data_ref.right));
                this + sub
            }
            // [(L!)     1]
            None => { 1 }
        }
    }

    // Calculates the min black nodes on a path:
    fn min_black_height(node: &NodeRef) -> u64 {
        match node.data {
            // (T! c l r)
            Some(ref data_ref) => {
                let data_ref = data_ref.borrow();
                // (+ (if (eq? c 'B) 1 0) (min (min-black-height l)
                //                  (min-black-height r)))
                let this = if node.col == Black { 1 } else { 0 };
                let sub = std::num::min(max_black_height(&data_ref.left), max_black_height(&data_ref.right));
                this + sub
            }
            // [(L!)     1]
            None => { 1 }
        }
    }

    max_black_height(node) == min_black_height(node)
}

// Does this tree contain a red child of red?
fn no_red_red(node: &NodeRef) -> bool {
    assert!(node.col == Red || node.col == Black);
    if !node.is_leaf() {
        let (l, r) = node.children();
        assert!(l.col == Red || l.col == Black);
        assert!(r.col == Red || r.col == Black);

        if node.col == Black {
            return no_red_red(&l) && no_red_red(&r);
        }

        if node.col == Red && l.col == Black && r.col == Black {
            return no_red_red(&l) && no_red_red(&r);
        }

        return false;
    } else {
        return true;
    }
}

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

#[test]
fn test_insert() {
    let map00 = PersistentRBT::new();

    let (map01, _new_entry01) = map00.clone().insert(1, 2);
    let (map10, _new_entry10) = map00.clone().insert(2, 4);
    let (map11, _new_entry11) = map01.clone().insert(2, 4);

    assert_find!(map00, 1, None);
    assert_find!(map00, 2, None);

    assert_find!(map01, 1, 2);
    assert_find!(map01, 2, None);

    assert_find!(map11, 1, 2);
    assert_find!(map11, 2, 4);

    assert_find!(map10, 1, None);
    assert_find!(map10, 2, 4);

    assert!(map00.balanced());
    assert!(map00.no_red_red());
    assert!(map01.balanced());
    assert!(map01.no_red_red());
    assert!(map10.balanced());
    assert!(map10.no_red_red());
    assert!(map11.balanced());
    assert!(map11.no_red_red());

    // assert_eq!(new_entry01, true);
    // assert_eq!(new_entry10, true);
    // assert_eq!(new_entry11, true);

    // assert_eq!(map00.len(), 0);
    // assert_eq!(map01.len(), 1);
    // assert_eq!(map10.len(), 1);
    // assert_eq!(map11.len(), 2);
}

#[test]
fn test_insert_overwrite() {
    let empty = PersistentRBT::new();
    let (mapA, _new_entryA) = empty.clone().insert(1, 2);
    let (mapB, _new_entryB) = mapA.clone().insert(1, 4);
    let (mapC, _new_entryC) = mapB.clone().insert(1, 6);

    assert_find!(empty, 1, None);
    assert_find!(mapA, 1, 2);
    assert_find!(mapB, 1, 4);
    assert_find!(mapC, 1, 6);

    assert!(empty.balanced());
    assert!(empty.no_red_red());
    assert!(mapA.balanced());
    assert!(mapA.no_red_red());
    assert!(mapB.balanced());
    assert!(mapB.no_red_red());
    assert!(mapC.balanced());
    assert!(mapC.no_red_red());

    // assert_eq!(new_entryA, true);
    // assert_eq!(new_entryB, false);
    // assert_eq!(new_entryC, false);

    // assert_eq!(empty.len(), 0);
    // assert_eq!(mapA.len(), 1);
    // assert_eq!(mapB.len(), 1);
    // assert_eq!(mapC.len(), 1);
}

#[test]
fn test_remove() {
    let (map00, _) = (PersistentRBT::new()
        .insert(1, 2)).first()
        .insert(2, 4);

    let (map01, _) = map00.clone().remove(&1);
    let (map10, _) = map00.clone().remove(&2);
    let (map11, _) = map01.clone().remove(&2);

    assert_find!(map00, 1, 2);
    assert_find!(map00, 2, 4);

    assert_find!(map01, 1, None);
    assert_find!(map01, 2, 4);

    assert_find!(map11, 1, None);
    assert_find!(map11, 2, None);

    assert_find!(map10, 1, 2);
    assert_find!(map10, 2, None);

    assert!(map00.balanced());
    assert!(map00.no_red_red());
    assert!(map01.balanced());
    assert!(map01.no_red_red());
    assert!(map10.balanced());
    assert!(map10.no_red_red());
    assert!(map11.balanced());
    assert!(map11.no_red_red());

    // assert_eq!(map00.len(), 2);
    // assert_eq!(map01.len(), 1);
    // assert_eq!(map10.len(), 1);
    // assert_eq!(map11.len(), 0);
}

#[test]
fn test_random() {
    use std::rand;
    use std::hashmap::HashSet;

    let mut values: HashSet<int> = HashSet::new();
    let mut rng = rand::rng();

    for _ in range(0, 10000) {
        values.insert(rand::Rand::rand(&mut rng));
    }

    let mut map = PersistentRBT::new();

    for &x in values.iter() {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }

    for &x in values.iter() {
        assert_find!(map, x, x);
    }

    for (i, x) in values.iter().enumerate() {
        if i % 2 == 0 {
            let (map1, _) = map.remove(x);
            assert!(map1.balanced());
            assert!(map1.no_red_red());
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

#[test]
fn test_insert_ascending() {
    let mut map = PersistentRBT::new();
    for x in range(0, 1000) {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }
}

#[test]
fn test_insert_descending() {
    let data = range(0, 1000).to_owned_vec();
    let mut map = PersistentRBT::new();
    for &x in data.rev_iter() {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }
}

#[test]
fn test_insert_descending_overwriting() {
    let data = range(0, 1000).to_owned_vec();
    let mut map = PersistentRBT::new();
    for &x in data.rev_iter() {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }

    for &x in data.rev_iter() {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }
}

#[test]
fn test_insert_ascending_overwriting() {
    let mut map = PersistentRBT::new();
    for x in range(0, 1000) {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }

    for x in range(0, 1000) {
        let (map1, _) = map.insert(x, x);
        assert!(map1.balanced());
        assert!(map1.no_red_red());
        map = map1;
    }
}

#[test]
fn test_insert_arrg() {
    let map0 = PersistentRBT::new();
    let (map1, _) = map0.clone().insert(-2355952011642934374, -2355952011642934374);
    let (map2, _) = map1.clone().insert(166785517347840485, 166785517347840485);
    let (map3, _) = map2.clone().insert(-6442202968218938924, -6442202968218938924);
    let (map4, _) = map3.clone().insert(7705992010132949805, 7705992010132949805);

    assert!(map0.balanced());
    assert!(map0.no_red_red());
    assert!(map1.balanced());
    assert!(map1.no_red_red());
    assert!(map2.balanced());
    assert!(map2.no_red_red());
    assert!(map3.balanced());
    assert!(map3.no_red_red());
    assert!(map4.balanced());
    assert!(map4.no_red_red());
}