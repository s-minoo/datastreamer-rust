use std::sync::Arc;

use tokio::sync::RwLock;

pub mod constant;

type SharedData = Arc<RwLock<Vec<String>>>;
