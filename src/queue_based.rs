use crate::interface::AbsIPCEntity;
use crate::vqueue::IPCItem;
use alloc::string::String;

pub struct QueueBasedEntity {
    queue_id: usize,
    default_dispatcher: bool,
    // todo
}

impl AbsIPCEntity for QueueBasedEntity {
    fn unregister(&mut self) -> Result<(), String> {
        todo!()
    }

    fn id(&self) -> u64 {
        todo!()
    }

    fn from_id(id: u64) -> Result<Self, String> {
        todo!()
    }

    fn send_to_inner(&self, item: IPCItem) -> Result<(), String> {
        todo!()
    }

    async fn recv_inner(&self, msg_type: u64) -> Result<IPCItem, String> {
        todo!()
    }
}
