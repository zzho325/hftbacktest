use futures_util::{SinkExt, stream::SplitSink};
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};

/// Sends a WebSocket subscription message to a specified channel with optional product ID and JWT
/// authentication.
pub async fn subscribe_ws(
    ws_write: &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    channel: &str,
    product_id: Option<&str>,
    jwt: &str,
) -> Result<(), tokio_tungstenite::tungstenite::Error> {
    let mut msg = serde_json::json!({
        "type" : "subscribe",
        "channel" : channel,
        "jwt": jwt
    });
    if let Some(pid) = product_id {
        msg["product_ids"] = serde_json::json!([pid])
    }
    ws_write.send(Message::Text(msg.to_string().into())).await
}
