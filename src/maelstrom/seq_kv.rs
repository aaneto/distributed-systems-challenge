use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum SeqKVRequest {
    #[serde(rename = "read")]
    Read(SeqKVReadRequest),
    #[serde(rename = "read-int")]
    ReadInt(SeqKVReadIntRequest),
    #[serde(rename = "cas")]
    CompareAndSwap(SeqKVCompareAndSwapRequest),
    #[serde(rename = "write")]
    Write(SeqKVWriteRequest),
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVReadRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub key: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVReadIntRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub key: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVCompareAndSwapRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub key: String,
    pub from: Option<u64>,
    pub to: Option<u64>,
    pub create_if_not_exists: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVWriteRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub key: String,
    pub value: u64,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVErrorResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub code: u64,
    pub text: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVNoDataResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct SeqKVReadResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub msg_id: Option<u64>,
    pub value: u64,
}
