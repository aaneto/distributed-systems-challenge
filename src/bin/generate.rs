use distributed_systems::maelstrom::*;
use serde::{Deserialize, Serialize};

fn main() {
    let mut id_count = 0;
    let node_id = get_node_id().unwrap();
    loop {
        node_loop(&node_id, &mut id_count).unwrap();
    }
}

fn generate_id(node_id: &str, current_count: u32) -> u64 {
    let mut acc = 0;

    for ch in node_id.chars() {
        acc += ch as u32;
    }

    ((acc as u64) << 32) + current_count as u64
}

fn node_loop(node_id: &str, current_count: &mut u32) -> Result<(), Box<dyn std::error::Error>> {
    let msg: NodeMessage<GenerateRequest> = read_node_message()?;
    let new_id = generate_id(node_id, *current_count);
    let new_msg: NodeMessage<GenerateResponse> = NodeMessage {
        dest: msg.src,
        src: node_id.to_string(),
        body: GenerateResponse {
            _type: "generate_ok".into(),
            id: new_id,
            in_reply_to: msg.body.msg_id,
        },
    };
    write_node_message(&new_msg)?;
    *current_count += 1;

    Ok(())
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GenerateRequest {
    #[serde(rename = "type")]
    pub _type: String,
    pub msg_id: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct GenerateResponse {
    #[serde(rename = "type")]
    pub _type: String,
    pub id: u64,
    pub in_reply_to: u64,
}
