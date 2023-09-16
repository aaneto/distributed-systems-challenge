use serde::{Deserialize, Serialize};
use distributed_systems::maelstrom::*;

fn main() {
    let node_id = get_node_id().unwrap();
    loop {
        node_loop(&node_id).unwrap();
    }
}

fn node_loop(node_id: &String) -> Result<(), Box<dyn std::error::Error>> {
    let msg: NodeMessage<EchoRequest> = read_node_message()?;
    let new_msg: NodeMessage<EchoResponse> = NodeMessage {
        dest: msg.src,
        src: node_id.clone(),
        body: EchoResponse {
            _type: "echo_ok".into(),
            in_reply_to: msg.body.msg_id,
            echo: msg.body.echo,
        },
    };
    write_node_message(&new_msg)?;

    Ok(())
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
