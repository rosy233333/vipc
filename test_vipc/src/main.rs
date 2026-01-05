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

const WORKER_NUM: usize = 1000;
const DATA_PER_WORKER: usize = 10;

static CLIENT: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static SERVER: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static CLIENT_ID: LazyInit<u64> = LazyInit::new();
static SERVER_ID: LazyInit<u64> = LazyInit::new();

fn main() {
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

    // server
    let server_thread = std::thread::spawn(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                for _ in 0..WORKER_NUM * DATA_PER_WORKER {
                    let (msg_type, rep_type, data) = SERVER.recv_any().await.unwrap();
                    SERVER.send(*CLIENT_ID, rep_type, msg_type, data).unwrap();
                }
            })
    });

    // client
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::spawn(CLIENT.default_dispatcher());

            let mut handles = Vec::new();
            for i in 0..WORKER_NUM {
                handles.push(tokio::spawn(async move {
                    for j in 0..DATA_PER_WORKER {
                        let rep = CLIENT
                            .call(*SERVER_ID, 42, i as u64, [j as u64; 8])
                            .await
                            .unwrap();
                        assert_eq!(rep.msg_type, i as u64);
                        assert_eq!(rep.sender, *SERVER_ID);
                        for k in 0..8 {
                            assert_eq!(rep.data[k], j as u64);
                        }
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });

    server_thread.join().unwrap();
    println!("Test passed!");
    drop(map);
}
