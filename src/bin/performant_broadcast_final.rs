use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

const WAIT_TIME: Duration = Duration::from_millis(120);
const READ_WAIT_TIME: Duration = Duration::from_millis(1850);

fn main() {
    let node_id = get_node_id().unwrap();
    let mut state = GlobalState {
        node_id,
        neighborhood: vec![],
        topology: HashMap::new(),
        values: HashSet::new(),
        past_broadcast: HashSet::new(),
        message_bus: MessageBus {
            neighborhoods: HashMap::new(),
        },
        customer_read_bus: CustomerBus {
            messages: VecDeque::new(),
        },
    };
    let (tx, rx) = channel();

    thread::spawn(move || loop {
        let request: NodeMessage<RequestType> =
            read_node_message().expect("Could not read request");
        tx.send(request).unwrap();
    });
    loop {
        if let Some(mut message) = state.customer_read_bus.pop() {
            message.body.messages = state.values.iter().cloned().collect();
            write_node_message(&message).expect("Cannot write resend message.");
            eprintln!(
                "{} [{}] Sent read_ok to {}: {:?}",
                get_ts(),
                state.node_id,
                message.dest,
                message.body.messages
            );
        }

        match rx.try_recv() {
            Ok(node_message) => {
                handle_message(node_message, &mut state).expect("Could not parse message");
            }
            Err(TryRecvError::Empty) => {
                if let Some(response) = state.message_bus.pick_message() {
                    write_node_message(response).expect("Cannot write resend message.");
                };
            }
            Err(TryRecvError::Disconnected) => panic!("Internal error"),
        }
    }
}

fn handle_message(
    request: NodeMessage<RequestType>,
    state: &mut GlobalState,
) -> Result<(), Box<dyn std::error::Error>> {
    match request.body {
        RequestType::ReadOk(read_ok) => {
            let ok_msgs: HashSet<u64> = read_ok.messages.into_iter().collect();
            let new_msgs: HashSet<u64> = ok_msgs.difference(&state.values).copied().collect();
            state.values = state.values.union(&new_msgs).copied().collect();

            eprintln!(
                "{} [{}] Received read_ok({:?}) from {}",
                get_ts(),
                state.node_id,
                state.values,
                request.src
            );

            if new_msgs.is_empty() {
                return Ok(());
            }

            for msg in new_msgs {
                for dst_node_id in state.neighborhood.iter() {
                    // Node is sending us broadcast, we don't need to broadcast to it.
                    state.message_bus.delete_message_checked(&request.src, msg);

                    if state.past_broadcast.contains(&msg) {
                        continue;
                    }

                    if dst_node_id == &state.node_id {
                        continue;
                    }
                    let broadcast_msg = NodeMessage {
                        src: state.node_id.clone(),
                        dest: dst_node_id.clone(),
                        body: BroadcastResponse {
                            _type: "broadcast".into(),
                            in_reply_to: None,
                            msg_id: None,
                            message: msg,
                        },
                    };

                    let is_master_to_master =
                        is_main_node(&dst_node_id) && is_main_node(&state.node_id);
                    // Only master-master messages are tracked and retried.
                    if is_master_to_master {
                        let new_message_opt =
                            state
                                .message_bus
                                .add_message(dst_node_id, msg, broadcast_msg.clone());
                        if let Some(new_message) = new_message_opt {
                            write_node_message(&new_message).unwrap();
                            eprintln!(
                                "{} [{}] Sent broadcast({}) to {} [read-sync]",
                                get_ts(),
                                state.node_id,
                                msg,
                                dst_node_id
                            );
                        }
                    } else {
                        write_node_message(&broadcast_msg).unwrap();
                        eprintln!(
                            "{} [{}] Sent broadcast({}) to {} [read-sync][no-tracking]",
                            get_ts(),
                            state.node_id,
                            msg,
                            dst_node_id
                        );
                    }
                }

                state.past_broadcast.insert(msg);
            }
        }
        RequestType::BroadcastOk(broadcast_ok) => {
            let msg = broadcast_ok.msg_id.unwrap();
            eprintln!(
                "{} [{}] Received broadcast_ok({}) from {}",
                get_ts(),
                state.node_id,
                msg,
                request.src
            );
            state.message_bus.delete_message(&request.src, msg);
        }
        RequestType::Read(read_body) => {
            eprintln!(
                "{} [{}] Received read from {}",
                get_ts(),
                state.node_id,
                request.src
            );
            let read_ok = NodeMessage {
                src: state.node_id.clone(),
                dest: request.src.clone(),
                body: ReadResponse {
                    _type: "read_ok".into(),
                    messages: state.values.iter().copied().collect(),
                    in_reply_to: read_body.msg_id,
                    msg_id: None,
                },
            };

            if is_customer_node(&request.src) {
                let mut read_replicate_nodes = HashSet::new();

                if is_main_node(&state.node_id) {
                    for replicate_node in state.neighborhood.iter() {
                        if replicate_node == &state.node_id {
                            continue;
                        }
                        read_replicate_nodes.insert(replicate_node.clone());
                    }
                } else {
                    let neighborhood_master = state.neighborhood.first().unwrap();
                    let neighborhood = state.topology.get(neighborhood_master).unwrap();
                    read_replicate_nodes.insert(neighborhood_master.clone());
                    for replicate_node in neighborhood.iter() {
                        if replicate_node == &state.node_id {
                            continue;
                        }
                        read_replicate_nodes.insert(replicate_node.clone());
                    }
                }

                for neighborhood_node_id in read_replicate_nodes {
                    if neighborhood_node_id == state.node_id {
                        continue;
                    }

                    let new_read = NodeMessage {
                        src: state.node_id.clone(),
                        dest: neighborhood_node_id.clone(),
                        body: RequestType::Read(ReadBody {
                            in_reply_to: None,
                            msg_id: None,
                        }),
                    };
                    write_node_message(&new_read).expect("Cannot write message.");
                    eprintln!(
                        "{} [{}] Sent replicate read to {}",
                        get_ts(),
                        state.node_id,
                        neighborhood_node_id
                    );
                }
                state.customer_read_bus.add(read_ok);
            } else {
                write_node_message(&read_ok).expect("Cannot write message.");
                eprintln!(
                    "{} [{}] Sent read_ok to {}: {:?}",
                    get_ts(),
                    state.node_id,
                    request.src,
                    read_ok.body.messages
                );
            }
        }
        RequestType::Broadcast(broadcast_request) => {
            eprintln!(
                "{} [{}] Received broadcast({}) from {}",
                get_ts(),
                state.node_id,
                broadcast_request.message,
                request.src
            );
            state.values.insert(broadcast_request.message);

            let is_customer = is_customer_node(&request.src);
            let is_master_broadcast = is_main_node(&request.src) && is_main_node(&state.node_id);

            if is_customer || is_master_broadcast {
                let n = NodeMessage {
                    src: state.node_id.clone(),
                    dest: request.src.clone(),
                    body: ResponseBody::Basic(BasicResponse {
                        _type: "broadcast_ok".into(),
                        in_reply_to: broadcast_request.msg_id,
                        msg_id: Some(broadcast_request.message),
                    }),
                };
                write_node_message(&n).expect("Cannot write message.");
                eprintln!(
                    "{} [{}] Sent broadcast_ok({}) to {}",
                    get_ts(),
                    state.node_id,
                    broadcast_request.message,
                    request.src
                );
            }

            // Node is sending us broadcast, we don't need to broadcast to it.
            state
                .message_bus
                .delete_message_checked(&request.src, broadcast_request.message);

            if state.past_broadcast.contains(&broadcast_request.message) {
                return Ok(());
            }

            for neighborhood_node_id in state.neighborhood.iter() {
                if neighborhood_node_id == &request.src {
                    continue;
                }
                let node = NodeMessage {
                    src: state.node_id.clone(),
                    dest: neighborhood_node_id.clone(),
                    body: BroadcastResponse {
                        _type: "broadcast".into(),
                        in_reply_to: None,
                        msg_id: None,
                        message: broadcast_request.message,
                    },
                };
                let is_master_to_master =
                    is_main_node(&neighborhood_node_id) && is_main_node(&state.node_id);
                // Only master-master messages are tracked and retried.
                if is_master_to_master {
                    let new_message_opt = state.message_bus.add_message(
                        neighborhood_node_id,
                        broadcast_request.message,
                        node.clone(),
                    );
                    if let Some(new_message) = new_message_opt {
                        write_node_message(&new_message).unwrap();
                        eprintln!(
                            "{} [{}] Sent broadcast({}) to {}",
                            get_ts(),
                            state.node_id,
                            broadcast_request.message,
                            neighborhood_node_id
                        );
                    }
                } else {
                    write_node_message(&node).unwrap();
                    eprintln!(
                        "{} [{}] Sent broadcast({}) to {} [no-tracking]",
                        get_ts(),
                        state.node_id,
                        broadcast_request.message,
                        neighborhood_node_id
                    );
                }
            }

            state.past_broadcast.insert(broadcast_request.message);
        }
        RequestType::Topology(topology) => {
            eprintln!(
                "{} [{}] Received topology from {}: {:?}",
                get_ts(),
                state.node_id,
                request.src,
                topology.topology
            );
            state.topology = topology.topology;
            let node_number: String = state.node_id.chars().skip(1).collect();
            state.neighborhood = match node_number.parse::<u64>().unwrap() {
                0 => vec!["n1", "n2", "n3", "n4", "n5"],
                1..=4 => vec!["n0"],
                5 => vec!["n0", "n6", "n7", "n8", "n9", "n10"],
                6..=9 => vec!["n5"],
                10 => vec!["n5", "n11", "n12", "n13", "n14", "n15"],
                11..=14 => vec!["n10"],
                15 => vec!["n10", "n16", "n17", "n18", "n19", "n20"],
                16..=19 => vec!["n15"],
                20 => vec!["n15", "n21", "n22", "n23", "n24"],
                21..=24 => vec!["n20"],
                _ => vec![],
            }
            .into_iter()
            .map(|v| v.to_string())
            .collect();
            state.message_bus.update_neighborhood(&state.neighborhood);
            eprintln!(
                "{} [{}] Ignoring Maelstrom topology, setting neighborhood: {:?}",
                get_ts(),
                state.node_id,
                state.neighborhood
            );

            let n = NodeMessage {
                src: state.node_id.clone(),
                dest: request.src.clone(),
                body: ResponseBody::Basic(BasicResponse {
                    _type: "topology_ok".into(),
                    in_reply_to: topology.msg_id,
                    msg_id: None,
                }),
            };
            write_node_message(&n).expect("Cannot write message.");
            eprintln!(
                "{} [{}] Sent topology_ok to {}",
                get_ts(),
                state.node_id,
                request.src
            );
        }
    };

    Ok(())
}

fn get_ts() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    format!("{}.{}", ts.as_secs(), ts.subsec_millis())
}

struct GlobalState {
    node_id: String,
    neighborhood: Vec<String>,
    topology: HashMap<String, Vec<String>>,
    values: HashSet<u64>,
    past_broadcast: HashSet<u64>,
    message_bus: MessageBus,
    customer_read_bus: CustomerBus,
}

#[derive(Debug, Clone)]
struct CustomerBus {
    messages: VecDeque<(Timer, NodeMessage<ReadResponse>)>,
}

impl CustomerBus {
    /// Add an element to the customer bus with a newly created timer.
    pub fn add(&mut self, message: NodeMessage<ReadResponse>) {
        self.messages.push_back((
            Timer {
                instant: Instant::now(),
                duration: READ_WAIT_TIME,
            },
            message,
        ));
    }

    /// Pop an element from the customer bus, this will happend if there is an element
    /// and if the timer is done.
    pub fn pop(&mut self) -> Option<NodeMessage<ReadResponse>> {
        if let Some((timer, _)) = self.messages.front() {
            if timer.is_done() {
                return self.messages.pop_front().map(|(_, m)| m);
            }
        }

        None
    }
}

#[derive(Debug, Clone)]
struct MessageBus {
    neighborhoods: HashMap<String, (Timer, HashMap<u64, NodeMessage<BroadcastResponse>>)>,
}

impl MessageBus {
    pub fn update_neighborhood(&mut self, neighborhood: &Vec<String>) {
        for node_id in neighborhood {
            self.neighborhoods.insert(
                node_id.clone(),
                (
                    Timer {
                        instant: Instant::now(),
                        duration: WAIT_TIME,
                    },
                    HashMap::new(),
                ),
            );
        }
    }

    /// Pick a message from the Bus. We should reset the timer every time we send
    /// a message from the Bus.
    pub fn pick_message(&mut self) -> Option<&NodeMessage<BroadcastResponse>> {
        for (timer, responses) in self.neighborhoods.values_mut() {
            if timer.is_done() {
                timer.reset();
                return responses.values().next();
            }
        }

        None
    }

    /// If we add a message, we are sending a message to a node. For politeness, we add a timer to send another
    /// message to this node. Unless we receive something from it.
    ///
    /// We also need to be sure this message wasnt sent before, returning Some when this is new.
    pub fn add_message(
        &mut self,
        node_id: &str,
        message_value: u64,
        message: NodeMessage<BroadcastResponse>,
    ) -> Option<NodeMessage<BroadcastResponse>> {
        let (timer, nodes) = self.neighborhoods.get_mut(node_id).unwrap();
        timer.reset();

        match nodes.insert(message_value, message.clone()) {
            Some(_) => None,
            None => Some(message),
        }
    }

    /// Remove message from a node specific slot.
    pub fn delete_message(&mut self, node_id: &str, message: u64) {
        let (_timer, nodes) = self.neighborhoods.get_mut(node_id).unwrap();
        nodes.remove(&message);
    }

    /// Remove message from a node specific slot.
    pub fn delete_message_checked(&mut self, node_id: &str, message: u64) {
        if let Some((_timer, nodes)) = self.neighborhoods.get_mut(node_id) {
            nodes.remove(&message);
        }
    }
}

#[derive(Debug, Clone)]
struct Timer {
    instant: Instant,
    duration: Duration,
}

impl Timer {
    pub fn is_done(&self) -> bool {
        self.instant.elapsed() > self.duration
    }

    pub fn reset(&mut self) {
        self.instant = Instant::now();
    }
}

fn is_customer_node(node_id: &str) -> bool {
    node_id.chars().next() == Some('c')
}

fn is_main_node(node_id: &str) -> bool {
    node_id == "n0" || node_id == "n5" || node_id == "n10" || node_id == "n15" || node_id == "n20"
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PendingBroadcast {
    src_node: String,
    message: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct BroadcastSent {
    destination_node: String,
    message: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
enum ResponseBody {
    Basic(BasicResponse),
    Broadcast(BroadcastResponse),
    Read(ReadResponse),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
enum RequestType {
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastBody),
    #[serde(rename = "read")]
    Read(ReadBody),
    #[serde(rename = "read_ok")]
    ReadOk(ReadOkBody),
    #[serde(rename = "topology")]
    Topology(TopologyBody),
    #[serde(rename = "broadcast_ok")]
    BroadcastOk(ReadBody),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct BroadcastBody {
    message: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ReadBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct TopologyBody {
    topology: HashMap<String, Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct BasicResponse {
    #[serde(rename = "type")]
    _type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ReadResponse {
    #[serde(rename = "type")]
    _type: String,
    messages: Vec<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct ReadOkBody {
    messages: Vec<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
struct BroadcastResponse {
    #[serde(rename = "type")]
    _type: String,
    message: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}
