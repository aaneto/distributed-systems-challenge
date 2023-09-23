pub mod seq_kv;

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
use std::io::Write;
use std::time::{Duration, Instant};

pub fn read_node_message<B>() -> Result<NodeMessage<B>, Box<dyn Error>>
where
    B: DeserializeOwned,
{
    let mut buffer = String::new();
    std::io::stdin().read_line(&mut buffer)?;
    // eprintln!("READ: {}", buffer);
    let node_input: NodeMessage<B> = serde_json::from_str(&buffer)?;
    Ok(node_input)
}

pub fn write_node_message<B>(response: &NodeMessage<B>) -> Result<(), Box<dyn Error>>
where
    B: Serialize,
{
    let text: String = serde_json::to_string(&response)?;
    // eprintln!("SENDING: {}", text);
    std::io::stdout().write_all(text.as_bytes())?;
    std::io::stdout().write_all(b"\n")?;
    std::io::stdout().flush()?;
    Ok(())
}

pub fn write_node_message_no_flush<B>(response: &NodeMessage<B>) -> Result<(), Box<dyn Error>>
where
    B: Serialize,
{
    let text: String = serde_json::to_string(&response)?;
    // eprintln!("SENDING: {}", text);
    std::io::stdout().write_all(text.as_bytes())?;
    std::io::stdout().write_all(b"\n")?;
    Ok(())
}

pub fn get_node_id() -> Result<String, Box<dyn Error>> {
    let msg: NodeMessage<InitRequest> = read_node_message()?;
    let new_msg: NodeMessage<InitResponse> = NodeMessage {
        dest: msg.src,
        src: msg.body.node_id,
        body: InitResponse {
            _type: "init_ok".into(),
            in_reply_to: msg.body.msg_id,
        },
    };

    write_node_message(&new_msg)?;

    Ok(new_msg.src)
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct NodeMessage<B> {
    pub src: String,
    pub dest: String,
    pub body: B,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct InitRequest {
    #[serde(rename = "type")]
    pub _type: String,
    pub msg_id: u64,
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct InitResponse {
    #[serde(rename = "type")]
    pub _type: String,
    pub in_reply_to: u64,
}

#[derive(Debug, Clone)]
pub struct Timer {
    instant: Instant,
    duration: Duration,
}

impl Timer {
    pub fn from_millis(millis: u64) -> Timer {
        Timer {
            instant: Instant::now(),
            duration: Duration::from_millis(millis),
        }
    }
    pub fn is_done(&self) -> bool {
        self.instant.elapsed() > self.duration
    }

    pub fn reset(&mut self) {
        self.instant = Instant::now();
    }
}

pub fn generate_id(node_id: &str, current_count: u32) -> u64 {
    let mut acc = 0;

    for ch in node_id.chars() {
        acc += ch as u32;
    }

    ((acc as u64) << 32) + current_count as u64
}
