use tokio::net::unix::SocketAddr;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Result;

async fn handle_connection(peer: SocketAddr, stream: TcpStream) -> Result<()> {
    Ok(())
}
