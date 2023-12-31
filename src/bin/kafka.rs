use std::collections::HashMap;
use std::sync::mpsc::{channel, TryRecvError};
use std::thread;

use distributed_systems::{kafka::*, maelstrom::*, *};

const POLL_SIZE: usize = 50;

fn main() {
    let node_id = get_node_id().unwrap();
    let mut state = GlobalState {
        node_id,
        log_entries: HashMap::new(),
    };
    let (tx, rx) = channel();

    thread::spawn(move || loop {
        let request: NodeMessage<RequestType> =
            read_node_message().expect("Could not read request");
        tx.send(request).unwrap();
    });
    loop {
        match rx.try_recv() {
            Ok(msg) => {
                state.handle_message(msg).expect("Could not parse message");
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => panic!("Internal error"),
        }
    }
}

struct GlobalState {
    node_id: String,
    log_entries: HashMap<String, Vec<SparseLogEntry>>,
}

struct SparseLogEntry {
    offset: u64,
    data: u64,
    commited: bool,
}

impl GlobalState {
    pub fn handle_message(
        &mut self,
        msg: NodeMessage<RequestType>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match msg.body {
            RequestType::SendRequest(send) => {
                eprintln!(
                    "{} [{}] Received send({}): {}-{}",
                    get_ts(),
                    self.node_id,
                    msg.dest,
                    send.msg,
                    send.key,
                );
                let mut new_offset = 0;

                self.log_entries
                    .entry(send.key)
                    .and_modify(|val| {
                        let last_element_offset = val
                            .last_mut()
                            .expect("Append only log should always have an element.")
                            .offset;
                        new_offset = last_element_offset + 1;
                        val.push(SparseLogEntry {
                            offset: last_element_offset + 1,
                            data: send.msg,
                            commited: false,
                        });
                    })
                    .or_insert(vec![SparseLogEntry {
                        offset: 0,
                        data: send.msg,
                        commited: false,
                    }]);

                let res = NodeMessage {
                    src: self.node_id.clone(),
                    dest: msg.src,
                    body: ResponseType::SendResponse(SendResponse {
                        offset: new_offset,
                        in_reply_to: send.msg_id,
                        msg_id: None,
                    }),
                };

                write_node_message(&res).expect("Cannot write resend message.");
                Ok(())
            }
            RequestType::PollRequest(poll) => {
                eprintln!(
                    "{} [{}] Received poll({}): {:?}",
                    get_ts(),
                    self.node_id,
                    msg.dest,
                    poll.offsets,
                );
                let mut msgs = HashMap::new();
                for (log_key, offset) in poll.offsets.iter() {
                    let data_points: Option<Vec<[u64; 2]>> = self.log_entries.get(log_key).map(|keys| {
                        keys.iter()
                            .filter(|k| k.offset >= *offset)
                            .take(POLL_SIZE)
                            .map(|k| [k.offset, k.data])
                            .collect()
                    });
                    msgs.insert(log_key.clone(), data_points.unwrap_or(vec![]));
                }

                let res = NodeMessage {
                    src: self.node_id.clone(),
                    dest: msg.src,
                    body: ResponseType::PollResponse(PollResponse {
                        msgs,
                        in_reply_to: poll.msg_id,
                        msg_id: None,
                    }),
                };

                write_node_message(&res).expect("Cannot write resend message.");

                Ok(())
            }
            RequestType::CommitOffsetsRequest(commit_offset) => {
                eprintln!(
                    "{} [{}] Received commit_offset({}): {:?}",
                    get_ts(),
                    self.node_id,
                    msg.dest,
                    commit_offset.offsets,
                );
                for (log_key, offset) in commit_offset.offsets.iter() {
                    if let Some(sparse_log) = self.log_entries.get_mut(log_key) {
                        for sparse_key in sparse_log.iter_mut() {
                            if sparse_key.offset <= *offset {
                                sparse_key.commited = true;
                            }
                        }
                    }
                }

                let res = NodeMessage {
                    src: self.node_id.clone(),
                    dest: msg.src,
                    body: ResponseType::CommitOffsetsResponse(SimpleMessage {
                        in_reply_to: commit_offset.msg_id,
                        msg_id: None,
                    }),
                };

                write_node_message(&res).expect("Cannot write resend message.");
                Ok(())
            },
            RequestType::ListCommitedOffsetsRequest(list_commit) => {
                eprintln!(
                    "{} [{}] Received list_commit({}): {:?}",
                    get_ts(),
                    self.node_id,
                    msg.dest,
                    list_commit.keys,
                );
                let mut offsets = HashMap::new();
                for log_key in list_commit.keys.iter() {
                    if let Some(sparse_log) = self.log_entries.get_mut(log_key) {
                        let mut last_commited = None;
                        for sparse_key in sparse_log.iter_mut() {
                            if sparse_key.commited {
                                last_commited = Some(sparse_key.offset);
                            } else {
                                break;
                            }
                        }
                        offsets.insert(log_key.clone(), last_commited.unwrap_or(0));
                    }
                }

                let res = NodeMessage {
                    src: self.node_id.clone(),
                    dest: msg.src,
                    body: ResponseType::ListCommitedOffsetsResponse(ListCommitedOffsetsResponse {
                        offsets,
                        in_reply_to: list_commit.msg_id,
                        msg_id: None,
                    }),
                };

                write_node_message(&res).expect("Cannot write resend message.");
                Ok(())
            },
        }
    }
}