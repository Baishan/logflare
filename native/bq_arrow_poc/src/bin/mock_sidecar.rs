//! Minimal local HTTP server for measuring the Elixir -> sidecar IPC hop in
//! isolation from any real encoding/BigQuery work (that's measured
//! separately in the library benchmarks). Reads the request body fully
//! (realistic - a real sidecar has to receive the whole payload too) and
//! replies with a tiny fixed ack, so whatever time is measured is the
//! transport overhead itself, not any processing on top of it.

use std::io::Read;
use tiny_http::{Response, Server};

fn main() {
    let port = std::env::var("MOCK_SIDECAR_PORT").unwrap_or_else(|_| "8933".to_string());
    let server = Server::http(format!("127.0.0.1:{port}")).expect("failed to bind");

    // Signals readiness to whatever spawned this process (e.g. an Elixir
    // Port), which should wait for this line instead of guessing with a
    // fixed sleep.
    println!("READY");
    use std::io::Write;
    std::io::stdout().flush().ok();

    for mut request in server.incoming_requests() {
        let mut body = Vec::new();
        request.as_reader().read_to_end(&mut body).ok();

        let response = Response::from_string(r#"{"ok":true}"#)
            .with_header(tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap());
        let _ = request.respond(response);
    }
}
