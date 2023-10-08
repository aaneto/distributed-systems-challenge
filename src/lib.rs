pub mod maelstrom;
pub mod kafka;

pub fn get_ts() -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap();
    format!("{}.{}", ts.as_secs(), ts.subsec_millis())
}