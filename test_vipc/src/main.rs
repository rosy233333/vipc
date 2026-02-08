use async_notification::interface::{Notification, NotificationIf};
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
use tokio::task::JoinHandle;
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
    test_ipc();
    // test_signal();
    // test_signal_2();
}

fn test_ipc() {
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
            // SERVER.init_once(QueueBasedLocalEntity::new(true, false, Some(pid as usize)).unwrap());
            SERVER.init_once(QueueBasedLocalEntity::new(false, false, None).unwrap());
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
            // CLIENT.init_once(QueueBasedLocalEntity::new(false, true, None).unwrap());
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

fn test_signal() {
    use tokio::task::JoinHandle;

    let mut ids: Vec<u64> = Vec::new();
    while let Some(id) = Notification::new_id_signal() {
        ids.push(id);
    }

    // ids = ids
    //     .into_iter()
    //     .filter(|id| {
    //         ![
    //             0x010000000000003f, // child panic，libc::kill返回非0
    //             0x0100000000000040, // child panic，libc::kill返回非0
    //         ]
    //         .contains(id)
    //     })
    //     .collect();

    for id in &ids {
        std::println!("{:#018x}", *id);
    }

    let ids_c = ids.clone();

    match unsafe { libc::fork() } {
        0 => {
            // child
            let parent = unsafe { libc::getppid() };
            println!("parent pid: {}", parent);
            // sig_recv(ids_c, 2);
            // sig_send(parent, ids_c, 1);
            sig_send(parent, ids_c[3..4].into(), 1);
        }
        -1 => panic!("Fork failed!"),
        child => {
            // parent
            std::thread::sleep(Duration::from_secs(1));
            println!("child pid: {}", child);

            // sig_send(child, ids.clone(), 1);
            sig_recv(ids, 1);

            // std::thread::sleep(Duration::from_secs(1));
            // sig_send(child, ids, 1);
            // std::thread::sleep(Duration::from_secs(1));

            unsafe {
                libc::kill(child as i32, libc::SIGINT);
            }
        }
    }
}

fn test_signal_2() {
    use tokio::task::JoinHandle;
    const SIGNAL_HIGH8: u64 = 0x01 << 56;

    let mut ids: Vec<u64> = Vec::new();
    // while let Some(id) = Notification::new_id_signal() {
    //     ids.push(id);
    // }
    for i in libc::SIGRTMIN()..=libc::SIGRTMAX() {
        if ![
            0x3f, // sender panic，libc::kill返回非0
            0x40, // sender panic，libc::kill返回非0
        ]
        .contains(&i)
        {
            ids.push((i as u64) | SIGNAL_HIGH8);
        }
    }

    // ids = ids
    //     .into_iter()
    //     .filter(|id| {
    //         ![
    //             0x010000000000003f, // child panic，libc::kill返回非0
    //             0x0100000000000040, // child panic，libc::kill返回非0
    //         ]
    //         .contains(id)
    //     })
    //     .collect();

    for id in &ids {
        std::println!("{:#018x}", *id);
    }

    let ids_c = ids.clone();

    match unsafe { libc::fork() } {
        0 => {
            // child
            let parent = unsafe { libc::getppid() };
            println!("parent pid: {}", parent);
            // sig_recv(ids_c, 2);
            // sig_send(parent, ids_c, 1);
            // sig_send(parent, ids_c[3..4].into(), 1);
            std::thread::sleep(Duration::from_secs(1));
            Notification::notify(parent as u64, ids_c[0]);
        }
        -1 => panic!("Fork failed!"),
        child => {
            // parent
            println!("child pid: {}", child);

            // sig_send(child, ids.clone(), 1);
            // sig_recv(ids, 1);

            // std::thread::sleep(Duration::from_secs(1));
            // sig_send(child, ids, 1);
            // std::thread::sleep(Duration::from_secs(1));

            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let mut actual_ids: Vec<u64> = Vec::new();
                    while let Some(id) = Notification::new_id_signal() {
                        actual_ids.push(id);
                    }
                    assert_eq!(ids.len(), actual_ids.len());
                    for i in 0..ids.len() {
                        assert_eq!(ids[i], actual_ids[i]);
                    }

                    std::thread::sleep(Duration::from_secs(2));
                    Notification::wait_on(ids[0]).await;

                    println!("Received signal on id {:#018x}", ids[0]);
                });

            unsafe {
                libc::kill(child as i32, libc::SIGINT);
            }
        }
    }
}

fn sig_recv(ids: Vec<u64>, num: usize) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let mut handles: Vec<JoinHandle<()>> = Vec::new();
            for id in ids {
                handles.push(tokio::spawn(async move {
                    std::println!("before block on id {:#018x}", id);
                    for _ in 0..num {
                        Notification::wait_on(id).await;
                        std::println!("after block on id {:#018x}", id);
                    }
                }));
            }

            for handle in handles {
                let _ = handle.await;
            }
        });
}

fn sig_send(pid: i32, ids: Vec<u64>, num: usize) {
    let mut line = String::new();
    for _ in 0..num {
        for id in &ids {
            // // 每次发信号前，等待键盘输入回车
            // let _ = std::io::stdin().read_line(&mut line).unwrap();
            Notification::notify(pid as u64, *id);
        }
    }
}
