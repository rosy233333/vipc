use async_notification::interface::{Notification, NotificationIf};
use core::panic;
use fork::*;
use lazyinit::LazyInit;
#[cfg(feature = "vdso")]
use libvqueue::{self as vqueue, MappingFlags, MemIf, PhysPagePtr};
use libvsched2::schedule::event_source::EventSource;
#[cfg(not(feature = "vdso"))]
use memmap2::MmapMut;
use std::{
    mem::{self, ManuallyDrop},
    pin::Pin,
    ptr,
    sync::{
        Arc,
        atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    thread,
    time::Duration,
};
use tokio::task::JoinHandle;
use vipc::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    queue_based::{QueueBasedLocalEntity, QueueBasedSharedEntity},
    sched_based::SchedBasedLocalEntity,
};
#[cfg(not(feature = "vdso"))]
use vqueue;

// #[cfg(feature = "vdso")]
// mod map;
// #[cfg(feature = "vdso")]
// use crate::map::map_vdso;

const WORKER_NUM: usize = 1000;
const DATA_PER_WORKER: usize = 10;

static QB_CLIENT: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static QB_SERVER: LazyInit<QueueBasedLocalEntity> = LazyInit::new();
static SB_CLIENT: LazyInit<SchedBasedLocalEntity> = LazyInit::new();
static SB_SERVER: LazyInit<SchedBasedLocalEntity> = LazyInit::new();
// static CLIENT_ID: LazyInit<u64> = LazyInit::new();
// static SERVER_ID: LazyInit<u64> = LazyInit::new();

struct ID {
    client: AtomicU64,
    server: AtomicU64,
}

// pub fn map_shared() -> Result<&'static mut [u8], ()> {
//     #[cfg(feature = "vdso")]
//     {
//         map_vdso()
//     }
//     #[cfg(not(feature = "vdso"))]
//     unsafe {
//         use std::ptr::NonNull;

//         let map_ptr = libc::mmap(
//             std::ptr::null_mut(),
//             vqueue::QUEUE_ARRAY_SIZE,
//             libc::PROT_READ | libc::PROT_WRITE,
//             libc::MAP_SHARED | libc::MAP_ANONYMOUS,
//             -1,
//             0,
//         );
//         if map_ptr == libc::MAP_FAILED {
//             log::error!("mmap vdso failed");
//             return Err(());
//         }
//         let map =
//             std::slice::from_raw_parts_mut(map_ptr as *mut () as *mut u8, vqueue::QUEUE_ARRAY_SIZE);

//         vqueue::set_queue_array_addr_and_init(NonNull::new(map.as_mut_ptr() as *mut ()).unwrap());
//     }
// }

fn main() {
    // test_queue_based();
    test_sched_based();
    // test_signal();
    // test_signal_2();
}

#[cfg(feature = "vdso")]
struct MemIfImpl;

#[cfg(feature = "vdso")]
#[crate_interface::impl_interface]
impl MemIf for MemIfImpl {
    #[doc = " 在地址空间中分配用于vDSO和vVAR的虚存区域（不需同时分配物理页面），返回指向首地址的指针。"]
    #[doc = " "]
    #[doc = " 保证size为build_vdso传入的config.page_size的整数倍。"]
    #[doc = " 要求返回的地址也为config.page_size的整数倍。"]
    fn valloc(_vspace: usize, size: usize) -> *mut u8 {
        let map_ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if map_ptr == libc::MAP_FAILED {
            panic!("mmap vdso failed");
        }
        map_ptr as *mut u8
    }

    #[doc = " 分配多块用于vDSO和vVAR的连续物理页，返回`PhysPagePtr`。"]
    #[doc = " "]
    #[doc = " 保证size为build_vdso传入的config.page_size的整数倍。"]
    #[doc = ""]
    #[doc = " 若需要实现vDSO和vVAR在多地址空间的共享，则需要在分配时使这块空间可被共享（即，可被多次`map`）。"]
    fn ppage_alloc(_size: usize) -> PhysPagePtr {
        0
    }

    #[doc = " 从`alloc`返回的虚存区域中，映射其中一块到某个物理页面并设置权限。"]
    #[doc = " "]
    #[doc = " 被映射的物理页面可能和其它地址空间共享，也可能由这个地址空间独占。"]
    #[doc = " "]
    #[doc = " 保证vaddr对齐到build_vdso传入的config.page_size；len为config.page_size的整数倍。"]
    #[doc = ""]
    #[doc = " `flags`可能包含：READ、WRITE、EXECUTE、USER。"]
    fn map(_vspace: usize, vaddr: *mut u8, _ppage: PhysPagePtr, size: usize, flags: MappingFlags) {
        let mut libc_flag = libc::PROT_READ;
        if flags.contains(MappingFlags::EXECUTE) {
            libc_flag |= libc::PROT_EXEC;
        }
        if flags.contains(MappingFlags::WRITE) {
            libc_flag |= libc::PROT_WRITE;
        }
        unsafe {
            if libc::mprotect(vaddr as _, size, libc_flag) == libc::MAP_FAILED as _ {
                panic!("vdso: mprotect res failed");
            }
        };
    }

    #[doc = " 重新设置已映射好的，虚拟首地址为`vspace`区域的权限。"]
    #[doc = " "]
    #[doc = " 保证vaddr对齐到build_vdso传入的config.page_size。"]
    fn change_protect(_vspace: usize, vaddr: *mut u8, size: usize, flags: MappingFlags) {
        let mut libc_flag = libc::PROT_READ;
        if flags.contains(MappingFlags::EXECUTE) {
            libc_flag |= libc::PROT_EXEC;
        }
        if flags.contains(MappingFlags::WRITE) {
            libc_flag |= libc::PROT_WRITE;
        }
        unsafe {
            if libc::mprotect(vaddr as _, size, libc_flag) == libc::MAP_FAILED as _ {
                panic!("vdso: mprotect res failed");
            }
        };
    }

    #[doc = " 获取`vspace`空间中`vaddr`地址对应的内核虚拟地址。"]
    #[doc = " （也就是当前代码可以直接访问的地址）"]
    fn get_kernel_vaddr(_vspace: usize, vaddr: *mut u8) -> *mut u8 {
        vaddr
    }

    #[doc = " 复制物理页指针，复制前后指向同一块物理页。复制后，参数和返回值对应的两个指针均需可用。"]
    #[doc = " "]
    #[doc = " 如果物理页使用RAII管理，则需调用其`clone`方法。"]
    #[doc = " "]
    #[doc = " 如果物理页不使用RAII管理，则可以直接返回参数。"]
    fn ppage_clone(_ppage: PhysPagePtr) -> PhysPagePtr {
        0
    }
}

// #[cfg(feature = "vdso")]
// impl MemIfImpl {
//     fn alloc(size: usize) -> *mut u8 {
//         let map_ptr = unsafe {
//             libc::mmap(
//                 std::ptr::null_mut(),
//                 size,
//                 libc::PROT_READ | libc::PROT_WRITE,
//                 libc::MAP_SHARED | libc::MAP_ANONYMOUS,
//                 -1,
//                 0,
//             )
//         };
//         if map_ptr == libc::MAP_FAILED {
//             panic!("mmap vdso failed");
//         }
//         map_ptr as *mut u8
//     }

//     fn protect(addr: *mut u8, len: usize, flags: libvqueue::MappingFlags) {
//         use libvqueue::MappingFlags;

//         let mut libc_flag = libc::PROT_READ;
//         if flags.contains(MappingFlags::EXECUTE) {
//             libc_flag |= libc::PROT_EXEC;
//         }
//         if flags.contains(MappingFlags::WRITE) {
//             libc_flag |= libc::PROT_WRITE;
//         }
//         unsafe {
//             if libc::mprotect(addr as _, len, libc_flag) == libc::MAP_FAILED as _ {
//                 panic!("vdso: mprotect res failed");
//             }
//         };
//     }
// }

fn test_queue_based() {
    env_logger::init();
    log::info!("Starting IPC test...");
    // #[cfg(feature = "vdso")]
    // let map = map_vdso().unwrap();
    #[cfg(feature = "vdso")]
    libvqueue::load_and_init(0);
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
            QB_SERVER
                .init_once(QueueBasedLocalEntity::new(true, false, Some(pid as usize)).unwrap());
            // QB_SERVER.init_once(QueueBasedLocalEntity::new(false, false, None).unwrap());
            let id = QB_SERVER.id();
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
                        let item = QB_SERVER.recv_any().await.unwrap();
                        log::info!(
                            "[Server] Received message: type={}, reply_type={}, data={:?}",
                            item.msg_type,
                            item.rep_type,
                            item.data
                        );
                        assert!(item.msg_type == 42);
                        QB_SERVER
                            .send(client_id, item.rep_type, item.msg_type, item.data.clone())
                            .unwrap();
                        log::info!(
                            "[Server] Sent reply: msg_type={}, rep_type={}, data={:?}",
                            item.rep_type,
                            item.msg_type,
                            item.data
                        );
                    }
                });
        }
        -1 => panic!("Fork failed!"),
        child => {
            // parent, client
            let pid = unsafe { libc::getpid() };
            QB_CLIENT
                .init_once(QueueBasedLocalEntity::new(true, true, Some(pid as usize)).unwrap());
            // QB_CLIENT.init_once(QueueBasedLocalEntity::new(false, true, None).unwrap());
            let id = QB_CLIENT.id();
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
                    tokio::spawn(QB_CLIENT.default_dispatcher());
                    log::info!("[Client] Dispatcher started");

                    let mut handles = Vec::new();
                    for i in 0..WORKER_NUM {
                        handles.push(tokio::spawn(async move {
                            for j in 0..DATA_PER_WORKER {
                                log::info!(
                                    "[Client] Sending message: worker=msg_type={}, data={:?}",
                                    i,
                                    [j as u64; 8]
                                );
                                let rep = QB_CLIENT
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
                #[cfg(not(feature = "vdso"))]
                libc::munmap(map.as_mut_ptr() as *mut () as *mut libc::c_void, map.len());
            }

            println!("Test passed!");
        }
    }
}

struct Task(Pin<Box<dyn Future<Output = ()>>>);

fn test_sched_based() {
    env_logger::init();
    log::info!("Starting IPC test...");
    // #[cfg(feature = "vdso")]
    // let map = map_vdso().unwrap();
    #[cfg(feature = "vdso")]
    libvqueue::load_and_init(0);
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

            SB_SERVER.init_once(SchedBasedLocalEntity::new(true, 0, 1).unwrap());
            // SERVER.init_once(QueueBasedLocalEntity::new(false, false, None).unwrap());
            let id = SB_SERVER.id();
            log::info!("server id: 0x{:016x}", id);
            id_ptr.server.store(id, Ordering::Release);

            thread::sleep(Duration::from_secs(1));
            let client_id = id_ptr.client.load(Ordering::Acquire);

            let mut sever_coroutine = None;
            let current_task_ptr = &mut sever_coroutine as *mut _ as *mut ();
            sever_coroutine.replace(Task(Box::pin(async move {
                log::info!("Into server async");
                for _ in 0..WORKER_NUM * DATA_PER_WORKER {
                    log::info!("[Server] waiting for message...");
                    let item = SB_SERVER.recv_any(current_task_ptr).await.unwrap();
                    log::info!(
                        "[Server] Received message: type={}, reply_type={}, data={:?}",
                        item.msg_type,
                        item.rep_type,
                        item.data
                    );
                    assert!(item.msg_type == 42);
                    SB_SERVER
                        .send(client_id, item.rep_type, item.msg_type, item.data.clone())
                        .unwrap();
                    log::info!(
                        "[Server] Sent reply: msg_type={}, rep_type={}, data={:?}",
                        item.rep_type,
                        item.msg_type,
                        item.data
                    );
                }
            })));

            let mut task_to_run = current_task_ptr;
            loop {
                if task_to_run != ptr::null_mut() {
                    if let Poll::Ready(_) = unsafe { &mut *(task_to_run as *mut Option<Task>) }
                        .as_mut()
                        .unwrap()
                        .0
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                    {
                        break;
                    }
                }
                let new_ptr = SB_SERVER.take_task(0).0;
                task_to_run = new_ptr as *mut ();
            }
        }
        -1 => panic!("Fork failed!"),
        child => {
            // parent, client
            let pid = unsafe { libc::getpid() };
            SB_CLIENT.init_once(SchedBasedLocalEntity::new(false, 0, 1).unwrap());
            // CLIENT.init_once(QueueBasedLocalEntity::new(false, true, None).unwrap());
            let id = SB_CLIENT.id();
            log::info!("client id: 0x{:016x}", id);
            id_ptr.client.store(id, Ordering::Release);

            thread::sleep(Duration::from_secs(1));
            let server_id = id_ptr.server.load(Ordering::Acquire);

            // let mut client_main_coroutine = ManuallyDrop::new(None);
            // let current_task_ptr = &mut client_main_coroutine as *mut _ as *mut ();
            // client_main_coroutine.replace(Task(Box::pin(async move {
            log::info!("Into client async");
            // tokio::spawn(SB_CLIENT.default_dispatcher());
            // log::info!("[Client] Dispatcher started");

            for i in 0..WORKER_NUM {
                let mut client_coroutine: ManuallyDrop<Box<Option<Task>>> =
                    ManuallyDrop::new(Box::new(None));
                let current_task_ptr = Box::as_mut(&mut client_coroutine) as *mut _ as *mut ();
                client_coroutine.replace(Task(Box::pin(async move {
                    for j in 0..DATA_PER_WORKER {
                        log::info!(
                            "[Client] Sending message: worker=msg_type={}, data={:?}",
                            current_task_ptr as usize,
                            [j as u64; 8]
                        );
                        let rep = SB_CLIENT
                            .call(
                                server_id,
                                42,
                                current_task_ptr as usize as u64,
                                [j as u64; 8],
                            )
                            .await
                            .unwrap();
                        log::info!(
                            "[Client] Received reply: worker={}, msg_type={}, sender={}, data={:?}",
                            current_task_ptr as usize,
                            rep.msg_type,
                            rep.sender,
                            rep.data
                        );
                        assert_eq!(rep.msg_type, current_task_ptr as usize as u64);
                        assert_eq!(rep.sender, server_id);
                        for k in 0..8 {
                            assert_eq!(rep.data[k], j as u64);
                        }
                    }
                    log::info!("Worker {} done!", i);
                })));
                // Poll一次，如果没完成则会从事件源中获取协程继续Poll。
                client_coroutine
                    .as_mut()
                    .as_mut()
                    .unwrap()
                    .0
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
            }
            // })));

            // let mut task_to_run = current_task_ptr;
            let mut finished = 0;
            while finished < WORKER_NUM {
                let new_ptr = SB_CLIENT.take_task(0).0;
                let task_to_run = new_ptr as *mut ();
                if task_to_run != ptr::null_mut() {
                    if let Poll::Ready(_) = unsafe { &mut *(task_to_run as *mut Option<Task>) }
                        .as_mut()
                        .unwrap()
                        .0
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                    {
                        finished += 1;
                    }
                }
            }

            log::info!("Test passed?");
            unsafe {
                libc::kill(child, libc::SIGTERM);
                #[cfg(not(feature = "vdso"))]
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
