use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::error::Error;
use std::io::Write;

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
