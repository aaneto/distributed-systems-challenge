use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

fn main() {
    let node = EchoNode { node_id: "".to_string() };
    run_node_event_loop(node);
}

impl MaelstromNode for EchoNode {
    type MessageBody = EchoRequest;

    fn initialize(&mut self, node_id: String) {
        self.node_id = node_id;
    }

    fn handle_message(&mut self, msg: NodeMessage<EchoRequest>) -> Result<(), Box<dyn std::error::Error>> {
        let new_msg: NodeMessage<EchoResponse> = NodeMessage {
            dest: msg.src,
            src: self.node_id.to_owned(),
            body: EchoResponse {
                _type: "echo_ok".into(),
                in_reply_to: msg.body.msg_id,
                echo: msg.body.echo,
            },
        };
        write_node_message(&new_msg)
    }
}

struct EchoNode {
    node_id: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EchoRequest {
    #[serde(rename = "type")]
    pub _type: String,
    pub msg_id: u64,
    pub echo: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct EchoResponse {
    #[serde(rename = "type")]
    pub _type: String,
    pub in_reply_to: u64,
    pub echo: String,
}
