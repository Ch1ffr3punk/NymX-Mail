use nym_sdk::mixnet::{self, MixnetMessageSender, Recipient};
use ripemd::{Digest, Ripemd160};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use tokio::sync::mpsc;

const FILE_TAG_LEN: usize = 22;
const HANDSHAKE_CHUNK_IDX: u32 = 0xFFFFFFFF;
const HANDSHAKE_TIMEOUT_SECS: u64 = 20;
const REPLY_TIMEOUT_SECS: u64 = 120;
const RESEND_REQUEST_PREFIX: &str = "RESEND:";
const MAX_PAYLOAD_BYTES: usize = 15000;

use crate::TaskMessage;

macro_rules! gui_log {
    ($tx:expr, $($arg:tt)*) => {
        { let _ = $tx.send(TaskMessage::Log(format!($($arg)*))); }
    };
}

pub async fn send_text_message(
    address: String,
    message: String,
    log_tx: mpsc::UnboundedSender<TaskMessage>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let start_time = Instant::now();
    let data = message.as_bytes();

    if data.len() > MAX_PAYLOAD_BYTES {
        gui_log!(log_tx, "[ERROR] Message too large: {} bytes (max {} bytes)", data.len(), MAX_PAYLOAD_BYTES);
        gui_log!(log_tx, "[ERROR] Please shorten your message or split it into multiple parts.");
        let _ = log_tx.send(TaskMessage::SendDone);
        return Ok(());
    }

    gui_log!(log_tx, "Connecting to Nym Mixnet (ephemeral)...");

    let mut client = mixnet::MixnetClientBuilder::new_ephemeral()
        .build()
        .unwrap()
        .connect_to_mixnet()
        .await
        .unwrap();

    let our_address = client.nym_address();
    gui_log!(log_tx, "Your Nym address: {}", our_address);
    gui_log!(log_tx, "Sending anonymously to: {}", address);

    let file_size_bytes = data.len() as u64;
    let total_chunks = 1;
    let filename = generate_random_filename();
    
    gui_log!(log_tx, "Generated filename: {}", filename);
    gui_log!(log_tx, "Message size: {} bytes", file_size_bytes);

    let sender_hash = ripemd160_bytes(data);
    gui_log!(log_tx, "Sender hash: {}", sender_hash);

    let target_address: Recipient = match address.parse() {
        Ok(addr) => addr,
        Err(e) => {
            gui_log!(log_tx, "Error: Invalid Nym address format ({})", e);
            client.disconnect().await;
            return Err("Invalid recipient address".into());
        }
    };

    gui_log!(log_tx, "Sending handshake...");
    let file_tag = generate_file_tag();
    let handshake_payload = build_handshake_payload(&file_tag);
    
    if let Err(e) = client.send_plain_message(target_address, handshake_payload).await {
        gui_log!(log_tx, "Failed to send handshake: {}", e);
        client.disconnect().await;
        return Ok(());
    }

    gui_log!(log_tx, "Waiting up to {}s for recipient response...", HANDSHAKE_TIMEOUT_SECS);
    
    let handshake_result = timeout(Duration::from_secs(HANDSHAKE_TIMEOUT_SECS), async {
        loop {
            if let Some(msgs) = client.wait_for_messages().await {
                if let Some(msg) = msgs.into_iter().find(|m| !m.message.is_empty()) {
                    return msg;
                }
            }
        }
    }).await;

    let handshake_ok = match handshake_result {
        Ok(reply) => {
            let reply_str = String::from_utf8_lossy(&reply.message);
            if reply_str.starts_with(&format!("READY:{}", file_tag)) {
                gui_log!(log_tx, "Recipient is online and ready!");
                true
            } else {
                gui_log!(log_tx, "Unexpected response from recipient");
                false
            }
        }
        Err(_) => {
            gui_log!(log_tx, "No response within {}s", HANDSHAKE_TIMEOUT_SECS);
            false
        }
    };

    if !handshake_ok {
        gui_log!(log_tx, "Recipient appears to be offline. Aborting.");
        client.disconnect().await;
        return Ok(());
    }

    gui_log!(log_tx, "Sending message chunk...");
    let chunk_header = build_chunk_header(0, total_chunks, file_size_bytes, &filename, &file_tag);
    let mut chunk_payload = chunk_header;
    chunk_payload.extend_from_slice(data);
    
    if let Err(e) = client.send_plain_message(target_address, chunk_payload.clone()).await {
        gui_log!(log_tx, "Failed to send chunk: {}", e);
        client.disconnect().await;
        return Ok(());
    }
    
    gui_log!(log_tx, "Chunk sent successfully.");
    gui_log!(log_tx, "Waiting for reply...");

    let start_wait = Instant::now();
    let mut reply_message = None;
    
    while start_wait.elapsed() < Duration::from_secs(REPLY_TIMEOUT_SECS) {
        if let Some(msgs) = client.wait_for_messages().await {
            for msg in msgs {
                if msg.message.is_empty() { continue; }
                let msg_str = String::from_utf8_lossy(&msg.message);
                if msg_str.starts_with(RESEND_REQUEST_PREFIX) {
                    gui_log!(log_tx, "Resend requested (handling gracefully).");
                    if let Err(e) = client.send_plain_message(target_address, chunk_payload.clone()).await {
                        gui_log!(log_tx, "Failed to resend: {}", e);
                    }
                } else {
                    reply_message = Some(msg);
                    break;
                }
            }
            if reply_message.is_some() { break; }
        }
    }

    let reply_message = match reply_message {
        Some(msg) => msg,
        None => {
            gui_log!(log_tx, "Timeout: No reply received.");
            client.disconnect().await;
            return Ok(());
        }
    };

    let (_reply_file_tag, reply_hash) = match parse_reply_payload(&reply_message.message) {
        Ok(parsed) => parsed,
        Err(e) => {
            gui_log!(log_tx, "Failed to parse reply: {}", e);
            client.disconnect().await;
            return Ok(());
        }
    };

    gui_log!(log_tx, "Receiver hash: {}", reply_hash);
    
    if sender_hash == reply_hash {
        gui_log!(log_tx, "Hashes match! Message delivered successfully.");
    } else {
        gui_log!(log_tx, "Hashes do not match!");
    }

    client.disconnect().await;
    
    let elapsed = start_time.elapsed();
    gui_log!(log_tx, "Done! Total time: {:.2}s", elapsed.as_secs_f64());
    
    Ok(())
}

fn build_handshake_payload(file_tag: &str) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&HANDSHAKE_CHUNK_IDX.to_be_bytes());
    payload.extend_from_slice(&0u32.to_be_bytes());
    payload.extend_from_slice(&0u64.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(file_tag.as_bytes());
    payload
}

fn build_chunk_header(chunk_idx: usize, total_chunks: usize, file_size: u64, filename: &str, file_tag: &str) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&(chunk_idx as u32).to_be_bytes());
    header.extend_from_slice(&(total_chunks as u32).to_be_bytes());
    header.extend_from_slice(&file_size.to_be_bytes());
    
    let filename_bytes = filename.as_bytes();
    header.extend_from_slice(&(filename_bytes.len() as u16).to_be_bytes());
    header.extend_from_slice(filename_bytes);
    header.extend_from_slice(file_tag.as_bytes());
    header
}

fn parse_reply_payload(payload: &[u8]) -> Result<(String, String), &'static str> {
    if payload.len() < FILE_TAG_LEN { return Err("Reply payload too short"); }
    let file_tag = String::from_utf8_lossy(&payload[..FILE_TAG_LEN]).to_string();
    let hash = String::from_utf8_lossy(&payload[FILE_TAG_LEN..]).to_string();
    Ok((file_tag, hash))
}

fn generate_file_tag() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..FILE_TAG_LEN).map(|_| {
        let idx = rng.gen_range(0..CHARSET.len());
        CHARSET[idx] as char
    }).collect()
}

fn generate_random_filename() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    let base: String = (0..12)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect();
    format!("{}.txt", base)
}

fn ripemd160_bytes(data: &[u8]) -> String {
    let mut hasher = Ripemd160::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
