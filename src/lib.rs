#![no_std]

extern crate alloc;

pub mod interface;
pub mod queue_based;
pub mod sched_based;

#[cfg(feature = "vdso")]
use libvqueue as vqueue;
#[cfg(not(feature = "vdso"))]
use vqueue;