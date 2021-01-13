use std::any::TypeId;
use std::mem;

#[derive(Debug)]
pub struct AnyBox {
    type_id: TypeId,
    raw: *mut u8,
    dropper: fn(*mut u8),
    cloner: fn(*mut u8) -> *mut u8,
}

unsafe impl Send for AnyBox {}
unsafe impl Sync for AnyBox {}

impl AnyBox {
    pub fn new<T: Clone + Send + Sync + 'static>(v: T) -> AnyBox {
        fn drop_it<U>(raw: *mut u8) {
            unsafe {
                let _ = Box::from_raw(mem::transmute::<*mut u8, *mut U>(raw));
            }
        }

        fn clone_it<U: Clone>(src: *mut u8) -> *mut u8 {
            unsafe {
                let b_src = Box::from_raw(mem::transmute::<*mut u8, *mut U>(src));
                let b_dst = b_src.clone();
                // Forget the original box because it's still actually owned by the
                // dynamic mailbox and should not be dropped here
                mem::forget(b_src);
                mem::transmute(Box::into_raw(b_dst))
            }
        }

        unsafe {
            let raw = Box::into_raw(Box::new(v));

            AnyBox {
                type_id: TypeId::of::<T>(),
                raw: mem::transmute(raw),
                dropper: drop_it::<T>,
                cloner: clone_it::<T>,
            }
        }
    }

    pub fn into_typed<T: Clone + Send + Sync + 'static>(mut self) -> Result<T, Self> {
        if self.type_id == TypeId::of::<T>() {
            let mut raw = std::ptr::null_mut();
            mem::swap(&mut self.raw, &mut raw);

            let b: Box<T>;
            unsafe {
                b = Box::from_raw(mem::transmute::<_, *mut T>(raw));
            }
            return Ok(*b);
        }

        Err(self)
    }
}

impl Drop for AnyBox {
    fn drop(&mut self) {
        if self.raw.is_null() {
            return;
        }
        // Drop the allocation
        unsafe {
            (self.dropper)(self.raw);
        }
        self.raw = std::ptr::null_mut();
    }
}

impl Clone for AnyBox {
    fn clone(&self) -> Self {
        AnyBox {
            raw: (self.cloner)(self.raw),
            type_id: self.type_id,
            cloner: self.cloner,
            dropper: self.dropper,
        }
    }
}
