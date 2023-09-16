use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;

use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

fn main() {
    let node_id = get_node_id().unwrap();
    let mut state = GlobalState {
        node_id,
        neighborhood: vec![],
        broadcast_sent: HashMap::new(),
        to_send: VecDeque::new(),
        to_broadcast: VecDeque::new(),
        values: HashSet::new(),
        broadcast_count: 0,
        pending_ack: HashMap::new(),
    };
    let (tx, rx) = channel();

    thread::spawn(move|| {
        loop {
            let request: NodeMessage<RequestType> = read_node_message().expect("Could not read request");
            tx.send(request).unwrap();
        }
    });

    loop {
        match rx.try_recv() {
            Ok(node_message) => {
                handle_message(node_message, &mut state).expect("Could not parse message");
            },
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Disconnected) => panic!("Internal error"),
        }

        if let Some(response) = state.to_send.pop_front() {
            write_node_message(&response).expect("Cannot write message.");
        }
        if let Some(response) = state.to_broadcast.pop_front() {
            write_node_message(&response).expect("Cannot write message.");
        }

        // for (_, node) in state.pending_ack.iter() {
        //     if state.neighborhood.contains(&node.dest) {
        //         write_node_message(node).expect("Cannot write ACK message");
        //         break;
        //     }
        // }
    }
}

fn handle_message(
    request: NodeMessage<RequestType>,
    state: &mut GlobalState
) -> Result<(), Box<dyn std::error::Error>> {
    match request.body {
        RequestType::BroadcastOk(broadcast_ok) => {
            let reply_to = broadcast_ok.in_reply_to.expect("Expect broadcast_ok to reply something.");

            if state.pending_ack.contains_key(&reply_to) {
                state.pending_ack.remove(&reply_to);
            }
        },
        RequestType::Read(read_body) => {
            state.to_send.push_back(
                NodeMessage {
                    src: state.node_id.clone(),
                    dest: request.src,
                    body: ResponseBody::Read(ReadResponse {
                        _type: "read_ok".into(),
                        messages: state.values.iter().map(|v| *v).collect(),
                        in_reply_to: read_body.msg_id,
                        msg_id: None,
                    })
                }
            );
        }
        RequestType::Broadcast(broadcast_request) => {
            state.values.insert(broadcast_request.message);

            state.to_send.push_back(NodeMessage {
                src: state.node_id.clone(),
                dest: request.src,
                body: ResponseBody::Basic(BasicResponse {
                    _type: "broadcast_ok".into(),
                    in_reply_to: broadcast_request.msg_id,
                    msg_id: None,
                })
            });

            for neighborhood_node_id in state.neighborhood.iter() {
                if let Some(already_sent) = state.broadcast_sent.get_mut(neighborhood_node_id) {
                    if already_sent.contains(&broadcast_request.message) {
                        continue;
                    } else {
                        already_sent.insert(broadcast_request.message);
                    }
                } else {
                    let mut hs = HashSet::new();
                    hs.insert(broadcast_request.message);
                    state.broadcast_sent.insert(neighborhood_node_id.clone(), hs);
                }

                let broadcast_id = generate_id(&state.node_id, state.broadcast_count);
                state.broadcast_count += 1;

                let node = NodeMessage {
                    src: state.node_id.clone(),
                    dest: neighborhood_node_id.clone(),
                    body: ResponseBody::Broadcast(BroadcastResponse {
                        _type: "broadcast".into(),
                        in_reply_to: None,
                        msg_id: Some(broadcast_id),
                        message: broadcast_request.message,
                    })
                };
                state.to_broadcast.push_back(node.clone());
                state.pending_ack.insert(broadcast_id, node);
            }
        }
        RequestType::Topology(mut topology) => {
            if topology.topology.contains_key(&state.node_id) {
                state.neighborhood = topology.topology.remove(&state.node_id).unwrap();
            }
            state.to_send.push_back(
                NodeMessage {
                    src: state.node_id.clone(),
                    dest: request.src,
                    body: ResponseBody::Basic(BasicResponse {
                        _type: "topology_ok".into(),
                        in_reply_to: topology.msg_id,
                        msg_id: None,
                    }),
                }
            );
        }
    };

    Ok(())
}

struct GlobalState {
    neighborhood: Vec<String>,
    broadcast_sent: HashMap<String, HashSet<u64>>,
    to_send: VecDeque<NodeMessage<ResponseBody>>,
    to_broadcast: VecDeque<NodeMessage<ResponseBody>>,
    /// Maps pending ack from message_id to message pending.
    pending_ack: HashMap<u64, NodeMessage<ResponseBody>>,
    node_id: String,
    values: HashSet<u64>,
    broadcast_count: u32,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
enum ResponseBody {
    Basic(BasicResponse),
    Broadcast(BroadcastResponse),
    Read(ReadResponse)
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

fn generate_id(node_id: &String, current_count: u32) -> u64 {
    let mut acc = 0;

    for ch in node_id.chars() {
        acc += ch as u32;
    }

    ((acc as u64) << 32) + current_count as u64
}