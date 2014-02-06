
use std::cast;

//=-------------------------------------------------------------------------------------------------
// NodeRef
//=-------------------------------------------------------------------------------------------------
pub trait RefCountedNode {
    fn get_ref_count(&self) -> uint;

    // Increments the refcount and returns the old value
    fn inc_ref_count(&self) -> uint;

    // Decrements the refcount and returns the old value
    fn dec_ref_count(&self) -> uint;

    fn destroy(&mut self);
}

pub struct NodeRef<TNode> {
    ptr: *mut TNode
}

pub enum NodeRefBorrowResult<'a, TNode> {
    OwnedNode(&'a mut TNode),
    SharedNode(&'a TNode),
}

impl<TNode: RefCountedNode> NodeRef<TNode> {
    pub fn borrow<'a>(&'a self) -> &'a TNode {
        unsafe {
            cast::transmute(self.ptr)
        }
    }

    pub fn borrow_mut<'a>(&'a mut self) -> &'a mut TNode {
        unsafe {
            assert!(self.borrow().get_ref_count() == 1);
            cast::transmute(self.ptr)
        }
    }

    pub fn try_borrow_owned<'a>(&'a mut self) -> NodeRefBorrowResult<'a, TNode> {
        unsafe {
            if self.borrow().get_ref_count() == 1 {
                OwnedNode(cast::transmute(self.ptr))
            } else {
                SharedNode(cast::transmute(self.ptr))
            }
        }
    }
}

#[unsafe_destructor]
impl<TNode: RefCountedNode> Drop for NodeRef<TNode> {
    fn drop(&mut self) {
        unsafe {
            let node: &mut TNode = cast::transmute(self.ptr);
            let old_count = node.dec_ref_count();
            assert!(old_count >= 1);
            if old_count == 1 {
                node.destroy()
            }
        }
    }
}

impl<TNode: RefCountedNode> Clone for NodeRef<TNode> {
    fn clone(&self) -> NodeRef<TNode> {
        unsafe {
            let old_count = (*self.ptr).inc_ref_count();
            assert!(old_count >= 1);
        }

        NodeRef { ptr: self.ptr }
    }
}

