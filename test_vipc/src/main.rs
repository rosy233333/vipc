use core::panic;
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
        atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering},
    },
    thread,
    time::Duration,
};
use vipc::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    queue_based::{QueueBasedLocalEntity, QueueBasedSharedEntity},
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
// static CLIENT_ID: LazyInit<u64> = LazyInit::new();
// static SERVER_ID: LazyInit<u64> = LazyInit::new();

struct ID {
    client: AtomicU64,
    server: AtomicU64,
}

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

    // CLIENT.init_once(QueueBasedLocalEntity::new(false, true, None).unwrap());
    // SERVER.init_once(QueueBasedLocalEntity::new(false, false, None).unwrap());
    // CLIENT_ID.init_once(CLIENT.id());
    // SERVER_ID.init_once(SERVER.id());
    // // CLIENT和SERVER会在fork中被复制（且未调用内部的clone），而两份拷贝均会操作共享数据，也因此会drop两遍共享数据。
    // // 因此需要手动增加引用计数。
    // mem::forget(unsafe { IPCSharedEntity::from_id(*CLIENT_ID) });
    // mem::forget(unsafe { IPCSharedEntity::from_id(*SERVER_ID) });
    // log::info!("Client ID: 0x{:016x}", *CLIENT_ID);
    // log::info!("Server ID: 0x{:016x}", *SERVER_ID);

    let id_ptr = unsafe {
        &mut *(libc::mmap(
            std::ptr::null_mut(),
            mem::size_of::<ID>(),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        ) as *mut () as *mut ID)
    };

    match unsafe { libc::fork() } {
        0 => {
            // child, server
            log::info!("Into server process");
            let pid = unsafe { libc::getpid() };
            SERVER.init_once(QueueBasedLocalEntity::new(true, false, Some(pid as usize)).unwrap());
            let id = SERVER.id();
            log::info!("server id: 0x{:016x}", id);
            id_ptr.server.store(id, Ordering::Release);

            thread::sleep(Duration::from_secs(1));
            let client_id = id_ptr.client.load(Ordering::Acquire);

            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    log::info!("Into server async");
                    for _ in 0..WORKER_NUM * DATA_PER_WORKER {
                        log::info!("[Server] waiting for message...");
                        let (msg_type, rep_type, data) = SERVER.recv_any().await.unwrap();
                        log::info!(
                            "[Server] Received message: type={}, reply_type={}, data={:?}",
                            msg_type,
                            rep_type,
                            data
                        );
                        assert!(msg_type == 42);
                        SERVER
                            .send(client_id, rep_type, msg_type, data.clone())
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
            let pid = unsafe { libc::getpid() };
            CLIENT.init_once(QueueBasedLocalEntity::new(true, true, Some(pid as usize)).unwrap());
            let id = CLIENT.id();
            log::info!("client id: 0x{:016x}", id);
            id_ptr.client.store(id, Ordering::Release);

            thread::sleep(Duration::from_secs(1));
            let server_id = id_ptr.server.load(Ordering::Acquire);

            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    log::info!("Into client async");
                    // tokio::spawn(CLIENT.default_dispatcher());
                    // log::info!("[Client] Dispatcher started");

                    let mut handles = Vec::new();
                    for i in 0..WORKER_NUM {
                        handles.push(tokio::spawn(async move {
                            for j in 0..DATA_PER_WORKER {
                                log::info!(
                                    "[Client] Sending message: worker=msg_type={}, data={:?}",
                                    i,
                                    [j as u64; 8]
                                );
                                let rep = CLIENT
                                    .call(server_id, 42, i as u64, [j as u64; 8])
                                    .await
                                    .unwrap();
                                log::info!(
                                    "[Client] Received reply: worker={}, msg_type={}, sender={}, data={:?}",
                                    i, rep.msg_type, rep.sender, rep.data
                                );
                                assert_eq!(rep.msg_type, i as u64);
                                assert_eq!(rep.sender, server_id);
                                for k in 0..8 {
                                    assert_eq!(rep.data[k], j as u64);
                                }
                            }
                            log::info!("Worker {} done!", i);
                        }));
                    }
                    log::info!("[Client] before join");
                    for handle in handles {
                        handle.await.unwrap();
                    }
                    log::info!("[Client] after join");
                });

            log::info!("Test passed?");
            unsafe {
                libc::kill(child, libc::SIGTERM);
                libc::munmap(map.as_mut_ptr() as *mut () as *mut libc::c_void, map.len());
            }

            println!("Test passed!");
        }
    }
}
