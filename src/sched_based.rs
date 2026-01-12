// use crate::interface::AbsIPCEntity;
use crate::{interface::SharedEntityIf, vqueue::IPCItem};
use alloc::string::String;

pub struct SchedBasedSharedEntity {
    // todo
}

// impl AbsIPCEntity for SchedBasedEntity {
//     fn unregister(&mut self) -> Result<(), String> {
//         todo!()
//     }

//     fn id(&self) -> u64 {
//         todo!()
//     }

//     fn from_id(id: u64) -> Result<Self, String> {
//         todo!()
//     }

//     fn send_to_inner(&self, item: IPCItem) -> Result<(), String> {
//         todo!()
//     }

//     async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String> {
//         todo!()
//     }
// }

impl SharedEntityIf for SchedBasedSharedEntity {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        // todo
        0
    }

    unsafe fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized,
    {
        // todo
        Err(String::from("Not implemented"))
    }

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String> {
        // todo
        Err(String::from("Not implemented"))
    }
}
