use fork::*;
use lazyinit::LazyInit;
#[cfg(feature = "vdso")]
use libvqueue as vqueue;
#[cfg(not(feature = "vdso"))]
use memmap2::MmapMut;
use std::{
    mem,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicUsize, Ordering},
    },
};
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

pub fn map_shared() -> Result<&'static mut [u8], ()> {
    #[cfg(feature = "vdso")]
    {
        map_vdso()
    }
    #[cfg(not(feature = "vdso"))]
    unsafe {
        use std::ptr::NonNull;

        let map_ptr = libc::mmap(
            std::ptr::null_mut(),
            vqueue::QUEUE_ARRAY_SIZE,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        );
        if map_ptr == libc::MAP_FAILED {
            log::error!("mmap vdso failed");
            return Err(());
        }
        let map =
            std::slice::from_raw_parts_mut(map_ptr as *mut () as *mut u8, vqueue::QUEUE_ARRAY_SIZE);

        vqueue::set_queue_array_addr_and_init(NonNull::new(map.as_mut_ptr() as *mut ()).unwrap());
    }
}

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

    CLIENT.init_once(QueueBasedLocalEntity::new(false, true).unwrap());
    SERVER.init_once(QueueBasedLocalEntity::new(false, false).unwrap());
    CLIENT_ID.init_once(CLIENT.id());
    SERVER_ID.init_once(SERVER.id());
    log::info!("Client ID: {:#16x}", *CLIENT_ID);
    log::info!("Server ID: {:#16x}", *SERVER_ID);

    match unsafe { libc::fork() } {
        0 => {
            // child, server
            log::info!("Into server process");
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    log::info!("Into server async");
                    for _ in 0..WORKER_NUM * DATA_PER_WORKER {
                        let (msg_type, rep_type, data) = SERVER.recv_any().await.unwrap();
                        log::info!(
                            "[Server] Received message: type={}, reply_type={}, data={:?}",
                            msg_type,
                            rep_type,
                            data
                        );
                        assert!(msg_type == 42);
                        SERVER
                            .send(*CLIENT_ID, rep_type, msg_type, data.clone())
                            .unwrap();
                        log::info!(
                            "[Server] Sent reply: msg_type={}, rep_type={}, data={:?}",
                            rep_type,
                            msg_type,
                            data
                        );
                    }
                });
        }
        -1 => panic!("Fork failed!"),
        child => {
            // parent, client
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    log::info!("Into client async");
                    tokio::spawn(CLIENT.default_dispatcher());
                    log::info!("[Client] Dispatcher started");

                    let mut handles = Vec::new();
                    for i in 0..WORKER_NUM {
                        handles.push(tokio::spawn(async move {
                            for j in 0..DATA_PER_WORKER {
                                log::info!(
                                    "[Client] Sending message: worker=msg_type={}, data={:?}",
                                    i, [j as u64; 8]
                                );
                                let rep = CLIENT
                                    .call(*SERVER_ID, 42, i as u64, [j as u64; 8])
                                    .await
                                    .unwrap();
                                log::info!(
                                    "[Client] Received reply: worker={}, msg_type={}, sender={}, data={:?}",
                                    i, rep.msg_type, rep.sender, rep.data
                                );
                                assert_eq!(rep.msg_type, i as u64);
                                assert_eq!(rep.sender, *SERVER_ID);
                                for k in 0..8 {
                                    assert_eq!(rep.data[k], j as u64);
                                }
                            }
                            log::info!("Worker {} done!", i);
                        }));
                    }
                    for handle in handles {
                        handle.await.unwrap();
                    }
                });

            log::info!("Test passed?");
            unsafe {
                libc::kill(child, libc::SIGTERM);
                libc::munmap(map.as_mut_ptr() as *mut () as *mut libc::c_void, map.len());
            }

            println!("Test passed!");
        }
    }

    // // server
    // let server_thread = std::thread::spawn(|| {
    //     tokio::runtime::Builder::new_current_thread()
    //         .enable_all()
    //         .build()
    //         .unwrap()
    //         .block_on(async {
    //             for _ in 0..WORKER_NUM * DATA_PER_WORKER {
    //                 let (msg_type, rep_type, data) = SERVER.recv_any().await.unwrap();
    //                 SERVER.send(*CLIENT_ID, rep_type, msg_type, data).unwrap();
    //             }
    //         })
    // });

    // // client
    // tokio::runtime::Builder::new_current_thread()
    //     .enable_all()
    //     .build()
    //     .unwrap()
    //     .block_on(async {
    //         tokio::spawn(CLIENT.default_dispatcher());

    //         let mut handles = Vec::new();
    //         for i in 0..WORKER_NUM {
    //             handles.push(tokio::spawn(async move {
    //                 for j in 0..DATA_PER_WORKER {
    //                     let rep = CLIENT
    //                         .call(*SERVER_ID, 42, i as u64, [j as u64; 8])
    //                         .await
    //                         .unwrap();
    //                     assert_eq!(rep.msg_type, i as u64);
    //                     assert_eq!(rep.sender, *SERVER_ID);
    //                     for k in 0..8 {
    //                         assert_eq!(rep.data[k], j as u64);
    //                     }
    //                 }
    //             }));
    //         }
    //         for handle in handles {
    //             handle.await.unwrap();
    //         }
    //     });
}
