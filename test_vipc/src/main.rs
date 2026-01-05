use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
    },
};

use lazyinit::LazyInit;
#[cfg(feature = "vdso")]
use libvqueue as vqueue;
use libvqueue::IPCItem;
#[cfg(not(feature = "vdso"))]
use memmap2::MmapMut;
use vipc::{
    interface::{LocalEntityIf, SharedEntityIf},
    queue_based::QueueBasedLocalEntity,
};
#[cfg(not(feature = "vdso"))]
use vqueue;

#[cfg(feature = "vdso")]
mod map;
#[cfg(feature = "vdso")]
use crate::map::map_vdso;

const QUEUE_NUM: usize = 16;
const WORKERS_PER_QUEUE: usize = 16;
const DATA_PER_WORKER: usize = 128;

static CLIENT: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static SERVER: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static CLIENT_ID: LazyInit<u64> = LazyInit::new();
static SERVER_ID: LazyInit<u64> = LazyInit::new();

fn main() {
    assert!(QUEUE_NUM <= vqueue::ARRAY_LEN);
    assert!(WORKERS_PER_QUEUE * DATA_PER_WORKER < vqueue::QUEUE_LEN);

    env_logger::init();
    log::info!("Starting IPC test...");
    #[cfg(feature = "vdso")]
    let map = map_vdso().expect("Failed to map VDSO");
    #[cfg(not(feature = "vdso"))]
    let map = {
        let mut map = MmapMut::map_anon(vqueue::QUEUE_ARRAY_SIZE).unwrap();
        unsafe {
            use std::ptr::NonNull;

            vqueue::set_queue_array_addr_and_init(
                NonNull::new(map.as_mut_ptr() as *mut ()).unwrap(),
            );
        }
        map
    };

    CLIENT.init_once(QueueBasedLocalEntity::new(true).unwrap());
    SERVER.init_once(QueueBasedLocalEntity::new(false).unwrap());
    CLIENT_ID.init_once(CLIENT.id());
    SERVER_ID.init_once(SERVER.id());

    // client
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut handles = Vec::new();
            for i in 0..1000 {
                handles.push(tokio::spawn(async move {
                    CLIENT
                        .send(*SERVER_ID, 42, [i as u64, 0, 0, 0, 0, 0, 0, 0])
                        .expect("Client send failed");
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    println!("Test passed!");
    drop(map);
}
