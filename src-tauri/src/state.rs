//! Unified Application State for SochDB Studio
//! 
//! Holds shared state for database connections and MCP server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use sochdb::connection::EmbeddedConnection;
use sochdb_mcp::McpServer;

use crate::policy::CommandPolicy;

/// Service status for UI display
#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceStatus {
    pub db_connected: bool,
    pub db_path: Option<String>,
    pub mcp_ready: bool,
}

impl Default for ServiceStatus {
    fn default() -> Self {
        Self {
            db_connected: false,
            db_path: None,
            mcp_ready: false,
        }
    }
}

/// Unified application state
pub struct AppState {
    /// Database connection (shared with MCP)
    pub connection: Arc<RwLock<Option<EmbeddedConnection>>>,
    /// MCP server instance
    pub mcp_server: Arc<RwLock<Option<McpServer>>>,
    /// Current database path
    pub db_path: Arc<RwLock<Option<PathBuf>>>,
    /// Time the current DB session was established
    pub connected_at: Arc<RwLock<Option<Instant>>>,
    /// Service status for UI
    pub status: Arc<RwLock<ServiceStatus>>,
    /// Command policy for access control
    pub policy: Arc<RwLock<CommandPolicy>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            connection: Arc::new(RwLock::new(None)),
            mcp_server: Arc::new(RwLock::new(None)),
            db_path: Arc::new(RwLock::new(None)),
            connected_at: Arc::new(RwLock::new(None)),
            status: Arc::new(RwLock::new(ServiceStatus::default())),
            policy: Arc::new(RwLock::new(CommandPolicy::default())),
        }
    }

    /// Get the current command policy
    pub async fn get_policy(&self) -> CommandPolicy {
        self.policy.read().await.clone()
    }

    /// Update the command policy
    pub async fn set_policy(&self, policy: CommandPolicy) {
        *self.policy.write().await = policy;
    }

    /// Initialize connection and MCP server for a database path
    pub async fn connect(&self, path: PathBuf) -> Result<(), String> {
        // Open database connection
        let conn = EmbeddedConnection::open(&path)
            .map_err(|e| format!("Failed to open database: {}", e))?;
        
        let conn_arc = Arc::new(conn);
        
        // Create MCP server with shared connection
        let mcp = McpServer::new(conn_arc.clone());
        
        // Update state
        {
            // We need to store the connection differently since MCP takes ownership
            // For now, we'll just track that we're connected
            let mut status = self.status.write().await;
            status.db_connected = true;
            status.db_path = Some(path.to_string_lossy().to_string());
            status.mcp_ready = true;
        }
        
        {
            let mut mcp_lock = self.mcp_server.write().await;
            *mcp_lock = Some(mcp);
        }
        
        {
            let mut path_lock = self.db_path.write().await;
            *path_lock = Some(path);
        }

        {
            let mut connected_at = self.connected_at.write().await;
            *connected_at = Some(Instant::now());
        }
        
        Ok(())
    }

    /// Disconnect from database
    pub async fn disconnect(&self) {
        let mut status = self.status.write().await;
        status.db_connected = false;
        status.db_path = None;
        status.mcp_ready = false;
        
        let mut mcp_lock = self.mcp_server.write().await;
        *mcp_lock = None;
        
        let mut conn_lock = self.connection.write().await;
        *conn_lock = None;
        
        let mut path_lock = self.db_path.write().await;
        *path_lock = None;

        let mut connected_at = self.connected_at.write().await;
        *connected_at = None;
    }

    /// Get current status
    pub async fn get_status(&self) -> ServiceStatus {
        self.status.read().await.clone()
    }

    /// Get seconds since the current DB session connected
    pub async fn uptime_seconds(&self) -> u64 {
        self.connected_at
            .read()
            .await
            .as_ref()
            .map(|started| started.elapsed().as_secs())
            .unwrap_or(0)
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
