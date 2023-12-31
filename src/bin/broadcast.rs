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
        values: HashSet::new(),

        to_send: VecDeque::new(),
        past_broadcast: HashSet::new(),
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
                if let Some(response) = state.to_send.pop_front() {
                    write_node_message(&response).expect("Cannot write message.");
                }
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
            state
                .past_broadcast
                .insert((request.src, broadcast_ok.msg_id.unwrap()));
        }
        RequestType::Read(read_body) => {
            let n = NodeMessage {
                src: state.node_id.clone(),
                dest: request.src,
                body: ResponseBody::Read(ReadResponse {
                    _type: "read_ok".into(),
                    messages: state.values.iter().copied().collect(),
                    in_reply_to: read_body.msg_id,
                    msg_id: None,
                }),
            };
            write_node_message(&n).expect("Cannot write message.");
        }
        RequestType::Broadcast(broadcast_request) => {
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

            for neighborhood_node_id in state.neighborhood.iter() {
                if state
                    .past_broadcast
                    .contains(&(neighborhood_node_id.clone(), broadcast_request.message))
                {
                    continue;
                }
                let node = NodeMessage {
                    src: state.node_id.clone(),
                    dest: neighborhood_node_id.clone(),
                    body: ResponseBody::Broadcast(BroadcastResponse {
                        _type: "broadcast".into(),
                        in_reply_to: None,
                        msg_id: None,
                        message: broadcast_request.message,
                    }),
                };

                write_node_message(&node).unwrap();
            }
        }
        RequestType::Topology(mut topology) => {
            if topology.topology.contains_key(&state.node_id) {
                state.neighborhood = topology.topology.remove(&state.node_id).unwrap();
            }
            let n = NodeMessage {
                src: state.node_id.clone(),
                dest: request.src,
                body: ResponseBody::Basic(BasicResponse {
                    _type: "topology_ok".into(),
                    in_reply_to: topology.msg_id,
                    msg_id: None,
                }),
            };
            write_node_message(&n).expect("Cannot write message.");
        }
    };

    Ok(())
}

struct GlobalState {
    node_id: String,
    neighborhood: Vec<String>,
    values: HashSet<u64>,

    to_send: VecDeque<NodeMessage<ResponseBody>>,
    past_broadcast: HashSet<(String, u64)>,
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
