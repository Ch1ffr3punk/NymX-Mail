use ssh2::Session;
use std::net::TcpStream;

pub fn connect_ssh(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Session, Box<dyn std::error::Error + Send + Sync>> {
    let mut session = Session::new()?;
    let tcp = TcpStream::connect((host, port))?;
    session.set_tcp_stream(tcp);
    session.handshake()?;
    session.userauth_password(username, password)?;
    
    if !session.authenticated() {
        return Err("Authentication failed".into());
    }
    
    Ok(session)
}
