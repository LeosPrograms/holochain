// mod network_mem;
// mod router;

use tokio::sync::mpsc;
use tokio::sync::oneshot;

use crate::memstore::HcNode;

pub type Path = String;
pub type Payload = String;
pub type ResponseTx = oneshot::Sender<String>;
pub type RequestTx = mpsc::UnboundedSender<(Path, Payload, ResponseTx)>;
pub type NodeId = HcNode;
pub type BasicNode = ();
