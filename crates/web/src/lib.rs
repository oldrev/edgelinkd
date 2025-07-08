pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

pub mod api;
pub mod handlers;
pub mod health;
pub mod models;
pub mod server;

pub use server::WebServer;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
