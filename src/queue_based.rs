use core::ops::Deref;

// use crate::interface::AbsIPCEntity;
use crate::{
    interface::{IPCSharedEntity, LocalEntityIf, SharedEntityIf},
    vqueue::IPCItem,
};
use alloc::string::String;

pub struct QueueBasedSharedEntity {
    queue_id: usize,
}

impl SharedEntityIf for QueueBasedSharedEntity {
    /// id的高8位需被保留，从而区分不同类型的IPC实体
    fn id(&self) -> u64 {
        self.queue_id as u64
    }

    fn from_id(id: u64) -> Result<Self, String>
    where
        Self: Sized,
    {
        Ok(Self {
            queue_id: id as usize,
        })
    }

    /// 发送消息给self
    fn send_to(&self, item: IPCItem) -> Result<(), String> {
        todo!()
    }
}

pub struct QueueBasedLocalEntity {
    shared: IPCSharedEntity,
    use_default_dispatcher: bool,
}

impl QueueBasedLocalEntity {
    pub fn new() -> Self {
        todo!()
    }
}

impl LocalEntityIf for QueueBasedLocalEntity {
    async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String> {
        todo!()
    }
}

impl Deref for QueueBasedLocalEntity {
    type Target = IPCSharedEntity;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}
