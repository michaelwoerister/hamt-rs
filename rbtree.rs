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

use std::rc::Rc;

#[deriving(Clone, Eq)]
enum Color {
    Red, Black, NegativeBlack, DoubleBlack
}

impl Color {
    fn inc(&self) -> Color {
        match *self {
            Black => DoubleBlack,
            NegativeBlack => Red,
            Red => Black,
            _ => fail!("Can't inc DoubleBlack")
        }
    }

    fn dec(&self) -> Color {
        match *self {
            Red => NegativeBlack,
            Black => Red,
            DoubleBlack => Black,
            NegativeBlack => fail!("Can't dec NegativeBlack")
        }
    }
}

struct NodeData {
    left: NodeRef,
    key: int,
    right: NodeRef,
}

#[deriving(Clone)]
struct NodeRef {
    col: Color,
    data: Option<Rc<NodeData>>
}


fn new_node(color: Color, left: NodeRef, key: int, right: NodeRef) -> NodeRef {
    NodeRef {
        col: color,
        data: Some(
            Rc::new(
                NodeData {
                    left: left,
                    key: key,
                    right: right
                }
            )
        )
    }
}

fn new_leaf(color: Color) -> NodeRef {
    assert!(color == Black || color == DoubleBlack);
    NodeRef { col: color, data: None }
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

    fn redden(self) -> NodeRef {
        NodeRef {
            col: Red,
            data: self.data
        }
    }
}

// WAT!
fn balance_node_ref(node_ref: NodeRef) -> NodeRef {
    if !node_ref.is_leaf() && (node_ref.col == Black || node_ref.col == DoubleBlack) {
        let result_col = node_ref.col.dec();
        let node_data = node_ref.get_data();
        let (left_child, right_child) = node_ref.children();

        if left_child.col == Red {
            // (T! (or 'B 'BB) (R (R a xk xv b) yk yv c) zk zv d)
            // (T! (or 'B 'BB) (R a xk xv (R b yk yv c)) zk zv d)
            let zk = node_data.key;
            let d = right_child;
            let (left_grand_child, right_grand_child) = left_child.children();

            if left_grand_child.col == Red {
                // (T! (or 'B 'BB) (R (R a xk xv b) yk yv c) zk zv d)
                let (a, b) = left_grand_child.children();
                let xk = left_grand_child.get_data().key;
                let yk = left_child.get_data().key;
                let c = right_grand_child;
                return create_case1(result_col, a, b, c, d, xk, yk, zk);
            } else if right_grand_child.col == Red {
                // (T! (or 'B 'BB) (R a xk xv (R b yk yv c)) zk zv d)
                let a = left_grand_child;
                let xk = left_child.get_data().key;
                let (b, c) = right_grand_child.children();
                let yk = right_grand_child.get_data().key;
                return create_case1(result_col, a, b, c, d, xk, yk, zk);
            }
        } else {
            // (T! (or 'B 'BB) a xk xv (R (R b yk yv c) zk zv d))
            // (T! (or 'B 'BB) a xk xv (R b yk yv (R c zk zv d))))
            let a = left_child;
            let xk = node_data.key;
            let (left_grand_child, right_grand_child) = right_child.children();

            if left_grand_child.col == Red {
                // (T! (or 'B 'BB) a xk xv (R (R b yk yv c) zk zv d))
                let (b, c) = left_grand_child.children();
                let yk = left_grand_child.get_data().key;
                let zk = right_child.get_data().key;
                let d = right_grand_child;
                return create_case1(result_col, a, b, c, d, xk, yk, zk);
            } else if right_grand_child.col == Red {
                // (T! (or 'B 'BB) a xk xv (R b yk yv (R c zk zv d))))
                let b = left_grand_child;
                let yk = right_child.get_data().key;
                let (c, d) = right_grand_child.children();
                let zk = right_grand_child.get_data().key;
                return create_case1(result_col, a, b, c, d, xk, yk, zk);
            }
        }
    }

    if !node_ref.is_leaf() && node_ref.col == DoubleBlack {
        // (BB a xk xv (-B (B b yk yv c) zk zv (and d (B))))
        let (a, right_child) = node_ref.children();
        let xk = node_ref.get_data().key;

        if right_child.col == NegativeBlack {
            assert!(!right_child.is_leaf());
            let (left_grand_child, right_grand_child) = right_child.children();

            if !left_grand_child.is_leaf() &&
               left_grand_child.col == Black &&
               right_grand_child.col == Black {
                let (b, c) = left_grand_child.children();
                let yk = left_grand_child.get_data().key;
                let zk = right_child.get_data().key;
                let d = right_grand_child;

                // (T cmp 'B (T cmp 'B a xk xv b) yk yv (balance cmp 'B c zk zv (redden d)))
                return new_node(
                    Black,
                    new_node(Black, a, xk, b),
                    yk,
                    balance_node_ref(new_node(Black, c, zk, d.redden()))
                );
            }
        }
    }

    if !node_ref.is_leaf() && node_ref.col == DoubleBlack {
        // (BB (-B (and a (B)) xk xv (B b yk yv c)) zk zv d)
        let (left_child, d) = node_ref.children();

        if left_child.col == NegativeBlack {
            assert!(!left_child.is_leaf());
            let (left_grand_child, right_grand_child) = left_child.children();

            if left_grand_child.col == Black &&
               !right_grand_child.is_leaf() &&
               right_grand_child.col == Black {
                let a = left_grand_child;
                let xk = left_child.get_data().key;
                let (b, c) = right_grand_child.children();
                let yk = right_grand_child.get_data().key;
                let zk = node_ref.get_data().key;

                // (T cmp 'B (balance cmp 'B (redden a) xk xv b) yk yv (T cmp 'B c zk zv d))]
                return new_node(
                    Black,
                    balance_node_ref(new_node(Black, a.redden(), xk, b)),
                    yk,
                    new_node(Black, c, zk, d)
                );
            }
        }
    }

    return node_ref;

    fn create_case1(color: Color,
                    a: NodeRef,
                    b: NodeRef,
                    c: NodeRef,
                    d: NodeRef,
                    xk: int,
                    yk: int,
                    zk: int) -> NodeRef {
        return new_node(color, new_node(Black, a, xk, b), yk, new_node(Black, c, zk, d));
    }
}
