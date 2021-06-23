use gaia_pegasus::ServerDetect;
use pegasus_network::Server;
use std::net::SocketAddr;

pub struct JnaServerDetector {

}

impl JnaServerDetector {
    pub fn new() -> Self {
        JnaServerDetector {

        }
    }

    pub fn update_peer_view(&self, peer_view: Vec<(u64, SocketAddr)>) {
        unimplemented!()
    }
}

impl ServerDetect for JnaServerDetector {
    fn fetch(&mut self) -> &[Server] {
        unimplemented!()
    }
}
