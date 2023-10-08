use std::collections::HashMap;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum RequestType {
    #[serde(rename = "send")]
    SendRequest(SendRequest),
    #[serde(rename = "poll")]
    PollRequest(PollRequest),
    #[serde(rename = "commit_offsets")]
    CommitOffsetsRequest(CommitOffsetsRequest),
    #[serde(rename = "list_committed_offsets")]
    ListCommitedOffsetsRequest(ListCommitedOffsetsRequest),
}

#[derive(Debug, Deserialize)]
pub struct SendRequest {
    pub key: String,
    pub msg: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct PollRequest {
    pub offsets: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CommitOffsetsRequest {
    pub offsets: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct ListCommitedOffsetsRequest {
    pub keys: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum ResponseType {
    #[serde(rename = "send_ok")]
    SendResponse(SendResponse),
    #[serde(rename = "poll_ok")]
    PollResponse(PollResponse),
    #[serde(rename = "commit_offsets_ok")]
    CommitOffsetsResponse(SimpleMessage),
    #[serde(rename = "list_committed_offsets_ok")]
    ListCommitedOffsetsResponse(ListCommitedOffsetsResponse),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SendResponse {
    pub offset: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct PollResponse {
    pub msgs: HashMap<String, Vec<[u64; 2]>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SimpleMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ListCommitedOffsetsResponse {
    pub offsets: HashMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}
