use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;
use std::time::{Duration, Instant};

use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

const WAIT_TIME: Duration = Duration::from_millis(200);

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
    };
    let (tx, rx) = channel();

    thread::spawn(move || loop {
        let request: NodeMessage<RequestType> =
            read_node_message().expect("Could not read request");
        tx.send(request).unwrap();
    });
    loop {
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
            let n = NodeMessage {
                src: state.node_id.clone(),
                dest: request.src.clone(),
                body: ResponseBody::Read(ReadResponse {
                    _type: "read_ok".into(),
                    messages: state.values.iter().copied().collect(),
                    in_reply_to: read_body.msg_id,
                    msg_id: None,
                }),
            };
            write_node_message(&n).expect("Cannot write message.");
            eprintln!(
                "{} [{}] Sent read_ok to {}",
                get_ts(),
                state.node_id,
                request.src
            );
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
            // if state.topology.contains_key(&state.node_id) {
            //     state.neighborhood = state.topology.remove(&state.node_id).unwrap();
            //     eprintln!(
            //         "{} [{}] Local topology: {:?}",
            //         get_ts(),
            //         state.node_id,
            //         state.neighborhood
            //     );
            //     state.message_bus.update_neighborhood(&state.neighborhood);
            // }
            let node_number: String = state.node_id.chars().skip(1).collect();
            state.neighborhood = match node_number.parse::<u64>().unwrap() {
                0 => vec!["n20", "n1", "n2", "n3", "n4", "n5"],
                1..=4 => vec!["n0"],
                5 => vec!["n0", "n6", "n7", "n8", "n9", "n10"],
                6..=9 => vec!["n5"],
                10 => vec!["n5", "n11", "n12", "n13", "n14", "n15"],
                11..=14 => vec!["n10"],
                15 => vec!["n10", "n16", "n17", "n18", "n19", "n20"],
                16..=19 => vec!["n15"],
                20 => vec!["n0", "n15", "n21", "n22", "n23", "n24"],
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

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum RequestType {
    #[serde(rename = "broadcast")]
    Broadcast(BroadcastBody),
    #[serde(rename = "read")]
    Read(ReadBody),
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
struct BroadcastResponse {
    #[serde(rename = "type")]
    _type: String,
    message: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
}
