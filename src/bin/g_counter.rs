use std::collections::VecDeque;
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;

use distributed_systems::maelstrom::seq_kv::*;
use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

const READ_OK_WAIT_MS: u64 = 400;
const PENDING_ADD_WAIT_MS: u64 = 200;
const NODE_COUNT: u64 = 3;

/*
1. SeqKV might hide state from the nodes. We need to sync all the nodes before a read.

We can have a replicate message sent on every cas_ok, this will generate len(network) messages
for every Add on the network. Meaning that if we have 10 nodes with 100 requests/second we will
generate 1000 messages + Ack. That sould not be a lot of data for a low-level system....

*/

fn main() {
    let node_id = get_node_id().unwrap();
    let (tx, rx) = channel();
    let mut handler = MaelstromHandler::new(node_id);
    let mut free_cycle_timer = Timer::from_millis(500);

    thread::spawn(move || loop {
        let request: NodeMessage<RequestType> =
            read_node_message().expect("Could not read request");
        tx.send(request).unwrap();
    });
    loop {
        match rx.try_recv() {
            Ok(node_message) => {
                handler
                    .handle_message(node_message)
                    .expect("Could not parse message");
            }
            Err(TryRecvError::Empty) => {
                if free_cycle_timer.is_done() {
                    handler.handle_free_cycle();
                    free_cycle_timer.reset();
                }
            }
            Err(TryRecvError::Disconnected) => panic!("Internal error"),
        }
    }
}

struct MaelstromHandler {
    node_id: String,
    count: u64,
    cas_id_counter: u64,
    pending_add: PendingAdd,
    pending_read_ok: VecDeque<PendingReadOk>,
    other_nodes: Vec<String>,
}

#[derive(Debug, Clone)]
struct PendingAdd {
    timer: Timer,
    msg_id: Option<u64>,
    value: u64,
}

impl PendingAdd {
    pub fn new(value: u64) -> PendingAdd {
        PendingAdd {
            timer: Timer::from_millis(PENDING_ADD_WAIT_MS),
            msg_id: None,
            value,
        }
    }
}

#[derive(Debug, Clone)]
struct PendingReadOk {
    timer: Timer,
    message_data: (String, Option<u64>),
}

impl MaelstromHandler {
    fn new(node_id: String) -> Self {
        let system_nodes = (0..NODE_COUNT)
            .map(|v| format!("n{v}"))
            .filter(|v| v != &node_id)
            .collect();
        MaelstromHandler {
            node_id: node_id.clone(),
            count: 0,
            cas_id_counter: 0,
            pending_add: PendingAdd::new(0),
            pending_read_ok: VecDeque::new(),
            other_nodes: system_nodes,
        }
    }

    fn handle_message(
        &mut self,
        request: NodeMessage<RequestType>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match request.body {
            RequestType::Add(body) => self.handle_add(request.src, body),
            RequestType::Read(body) => self.handle_read(request.src, body),
            RequestType::SeqKVError(err) => self.handle_seq_kv_error(err),
            RequestType::CasOk(cas_ok) => self.handle_cas_ok(cas_ok),
            RequestType::ReadOk(read_ok) => self.handle_read_ok(read_ok),
        }
    }

    fn handle_read_ok(
        &mut self,
        read_ok: SeqKVReadResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!(
            "{} [{}] Received seq_kv_read_ok({})",
            get_ts(),
            self.node_id,
            self.count
        );
        if read_ok.value > self.count {
            self.count = read_ok.value;
            eprintln!(
                "{} [{}] replaced count with read_ok value: {}",
                get_ts(),
                self.node_id,
                self.count
            )
        }
        Ok(())
    }

    fn handle_cas_ok(
        &mut self,
        cas_ok: SeqKVNoDataResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if cas_ok.in_reply_to == self.pending_add.msg_id {
            self.count += self.pending_add.value;
            self.pending_add.value = 0;
            self.pending_add.msg_id = None;
        } else {
            panic!("We should not received CAS message from other nodes.");
        }

        eprintln!(
            "{} [{}] Received seq_kv_cas_ok, new count: {}",
            get_ts(),
            self.node_id,
            self.count
        );

        for n_id in self.other_nodes.iter() {
            self.send_read_ok(n_id, None);
        }

        Ok(())
    }

    fn handle_free_cycle(&mut self) {
        eprintln!(
            "{} [{}] Pending to Add: {}",
            get_ts(),
            self.node_id,
            self.pending_add.value
        );

        let has_pending_send_ok = self
            .pending_read_ok
            .front()
            .map_or(false, |p_rok| p_rok.timer.is_done());
        if has_pending_send_ok {
            if let Some(pending_read_ok) = self.pending_read_ok.pop_front() {
                let (source, msg_id) = pending_read_ok.message_data;
                self.send_read_ok(&source, msg_id);
                return;
            }
        }

        let new_id = self.get_id();
        if self.pending_add.value > 0 && self.pending_add.timer.is_done() {
            self.send_seq_kv_compare_and_swap(
                Some(self.count),
                Some(self.count + self.pending_add.value),
                new_id,
            );
            self.pending_add.msg_id = Some(new_id);
            self.pending_add.timer.reset();
        }

    }

    fn handle_seq_kv_error(
        &mut self,
        err: SeqKVErrorResponse,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if err.in_reply_to == self.pending_add.msg_id && err.code == 22 {
            self.pending_add.msg_id = None;
            self.send_seq_kv_read();
        } else {
            eprintln!("{} [{}] seq-kv error: {:?}", get_ts(), self.node_id, err);
        }

        Ok(())
    }

    fn handle_add(&mut self, src: String, body: AddBody) -> Result<(), Box<dyn std::error::Error>> {
        let msg_id = self.get_id();

        eprintln!(
            "{} [{}] Received add({}) from {}",
            get_ts(),
            self.node_id,
            body.delta,
            src
        );

        let add_ok = NodeMessage {
            src: self.node_id.clone(),
            dest: src.clone(),
            body: AddResponse {
                _type: "add_ok".into(),
                in_reply_to: body.msg_id,
                msg_id: None,
            },
        };
        self.send_add_ok(&src, add_ok);

        if body.delta == 0 {
            return Ok(());
        }

        self.pending_add.value += body.delta;

        let from = if self.count == 0 {
            None
        } else {
            Some(self.count)
        };
        let to = Some(self.count + self.pending_add.value);
        self.send_seq_kv_compare_and_swap(from, to, msg_id);

        self.pending_add.msg_id = Some(msg_id);


        Ok(())
    }

    fn handle_read(
        &mut self,
        src: String,
        body: ReadBody,
    ) -> Result<(), Box<dyn std::error::Error>> {
        eprintln!(
            "{} [{}] Received read from {}, replying soon.",
            get_ts(),
            self.node_id,
            src.clone()
        );
        self.pending_read_ok.push_back(PendingReadOk {
            timer: Timer::from_millis(READ_OK_WAIT_MS),
            message_data: (src, body.msg_id),
        });
        // self.send_seq_kv_read(); // Send a read to sync data before sending read_ok.
        Ok(())
    }

    fn send_seq_kv_read(&self) {
        let seq_kv_read = NodeMessage {
            src: self.node_id.clone(),
            dest: "seq-kv".to_string(),
            body: SeqKVRequest::Read(SeqKVReadRequest {
                in_reply_to: None,
                msg_id: None,
                key: "sum".to_string(),
            }),
        };
        write_node_message(&seq_kv_read).expect("Cannot write resend message.");
        eprintln!("{} [{}] Sent seq_kv_read", get_ts(), self.node_id);
    }

    fn send_seq_kv_compare_and_swap(&self, from: Option<u64>, to: Option<u64>, msg_id: u64) {
        let seq_kv_cas = NodeMessage {
            src: self.node_id.clone(),
            dest: "seq-kv".to_string(),
            body: SeqKVRequest::CompareAndSwap(SeqKVCompareAndSwapRequest {
                in_reply_to: None,
                msg_id: Some(msg_id),
                key: "sum".to_string(),
                from,
                to,
                create_if_not_exists: true,
            }),
        };
        write_node_message(&seq_kv_cas).expect("Cannot write resend message.");
        eprintln!(
            "{} [{}] Sent seq_kv_cas({:?},{:?})",
            get_ts(),
            self.node_id,
            from,
            to,
        );
    }

    fn send_add_ok(&self, dst: &str, add_ok: NodeMessage<AddResponse>) {
        write_node_message(&add_ok).expect("Cannot write resend message.");
        eprintln!("{} [{}] Sent add_ok to {}", get_ts(), self.node_id, dst);
    }

    fn send_read_ok(&self, dst: &str, in_reply_to: Option<u64>) {
        let response = NodeMessage {
            src: self.node_id.clone(),
            dest: dst.to_string(),
            body: ReadResponse {
                _type: "read_ok".into(),
                in_reply_to,
                msg_id: None,
                value: self.count,
            },
        };
        write_node_message(&response).expect("Cannot write read_ok message.");
        eprintln!("{} [{}] Sent read_ok to {}", get_ts(), self.node_id, dst);
    }

    fn get_id(&mut self) -> u64 {
        self.cas_id_counter += 1;
        generate_id(&self.node_id, self.cas_id_counter as u32)
    }
}

fn get_ts() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    format!("{}.{}", ts.as_secs(), ts.subsec_millis())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
enum RequestType {
    #[serde(rename = "add")]
    Add(AddBody),
    #[serde(rename = "read")]
    Read(ReadBody),
    #[serde(rename = "error")]
    SeqKVError(SeqKVErrorResponse),
    #[serde(rename = "cas_ok")]
    CasOk(SeqKVNoDataResponse),
    #[serde(rename = "read_ok")]
    ReadOk(SeqKVReadResponse),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct AddBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    delta: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ReadBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ReadResponse {
    #[serde(rename = "type")]
    _type: String,
    value: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct AddResponse {
    #[serde(rename = "type")]
    _type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}
