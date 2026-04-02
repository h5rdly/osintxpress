use grammers_client::message::Message;
use grammers_client::tl::enums::MessageEntity;

pub fn format_telegram_message(message: &Message) -> String {
    let raw_text = message.text();
    
    // Translatem UTF-16 offsets to UTF-8 byte indices
    let mut utf16_to_utf8 = Vec::with_capacity(raw_text.len() + 1);
    for (byte_idx, ch) in raw_text.char_indices() {
        for _ in 0..ch.len_utf16() {
            utf16_to_utf8.push(byte_idx);
        }
    }
    // Push the final byte length to handle entities that go to the exact end of the string
    utf16_to_utf8.push(raw_text.len());

    let mut links = Vec::new();

    if let Some(entities) = message.fmt_entities() {
        for entity in entities {
            match entity {
                MessageEntity::TextUrl(t) => {
                    links.push((t.offset as usize, t.length as usize, t.url.clone()));
                }
                MessageEntity::Url(t) => {
                    let offset = t.offset as usize;
                    let length = t.length as usize;
                    
                    // Safely extract the raw URL string using our translation map
                    if offset + length < utf16_to_utf8.len() {
                        let start_byte = utf16_to_utf8[offset];
                        let end_byte = utf16_to_utf8[offset + length];
                        let url = &raw_text[start_byte..end_byte];
                        
                        let href = if !url.starts_with("http") { format!("https://{}", url) } else { url.to_string() };
                        links.push((offset, length, href));
                    }
                }
                _ => {} 
            }
        }
    }

    links.sort_by(|a, b| b.0.cmp(&a.0));

    let mut final_text = raw_text.to_string();

    // Inject HTML tags into the UTF-8 string
    for (utf16_offset, utf16_length, url) in links {
        if utf16_offset + utf16_length < utf16_to_utf8.len() {
            let start_byte = utf16_to_utf8[utf16_offset];
            let end_byte = utf16_to_utf8[utf16_offset + utf16_length];
            
            let open_tag = format!("<a href=\"{}\" target=\"_blank\" style=\"color: #0096ff; text-decoration: none;\">", url);
            let close_tag = "</a>";
            
            final_text.insert_str(end_byte, close_tag);
            final_text.insert_str(start_byte, &open_tag);
        }
    }

    final_text
}


pub fn build_message(msg: &Message, target_channel: &str, media_path: &str) -> String {
    
    let rich_text = format_telegram_message(msg);
    
    let escaped_text = rich_text
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r");
        
    let media_path = media_path.replace('\\', "\\\\"); // Windows path safety

    format!(
        r#"{{"channel": "{}", "text": "{}", "date": {}, "media": "{}"}}"#,
        target_channel, escaped_text, msg.date().timestamp(), media_path
    )
}