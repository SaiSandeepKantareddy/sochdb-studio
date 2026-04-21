//! Query execution commands

use serde::{Deserialize, Serialize};
use tauri::State;
use std::sync::Arc;
use crate::state::AppState;

/// Result of a query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub stats: QueryStats,
}

/// Query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub row_count: usize,
    pub execution_time_ms: f64,
    pub scanned_rows: usize,
}

/// Execute a SQL/SochQL query
#[tauri::command]
pub async fn execute_query(
    state: State<'_, Arc<AppState>>,
    query: String,
) -> Result<QueryResult, String> {
    let status = state.get_status().await;
    if !status.db_connected {
        return Err("No active connection".to_string());
    }

    let start = std::time::Instant::now();
    
    // Use MCP tools/call for query execution
    let mcp_lock = state.mcp_server.read().await;
    let mcp = mcp_lock.as_ref().ok_or("MCP server not initialized")?;
    
    // Create query request
    let req = sochdb_mcp::RpcRequest {
        jsonrpc: "2.0".to_string(),
        id: serde_json::Value::Number(1.into()),
        method: "tools/call".to_string(),
        params: serde_json::json!({
            "name": "sochdb_query",
            "arguments": { "query": query, "format": "json" }
        }),
    };
    
    let resp = mcp.dispatch(&req);
    let execution_time = start.elapsed().as_secs_f64() * 1000.0;
    
    match resp.result {
        Some(result) => {
            let text = result
                .get("content")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.get("text"))
                .and_then(|v| v.as_str())
                .unwrap_or("");

            let (columns, parsed_rows): (Vec<String>, Vec<Vec<serde_json::Value>>) =
                match serde_json::from_str::<serde_json::Value>(text) {
                    Ok(serde_json::Value::Array(items)) => {
                        if let Some(first_object) = items.first().and_then(|item| item.as_object()) {
                            let mut columns: Vec<String> = first_object.keys().cloned().collect();
                            columns.sort();
                            let rows = items
                                .into_iter()
                                .map(|item| {
                                    if let Some(obj) = item.as_object() {
                                        columns
                                            .iter()
                                            .map(|col| obj.get(col).cloned().unwrap_or(serde_json::Value::Null))
                                            .collect()
                                    } else {
                                        vec![item]
                                    }
                                })
                                .collect();
                            (columns, rows)
                        } else {
                            (
                                vec!["result".to_string()],
                                items.into_iter().map(|item| vec![item]).collect(),
                            )
                        }
                    }
                    Ok(value) => (vec!["result".to_string()], vec![vec![value]]),
                    Err(_) => (
                        vec!["result".to_string()],
                        vec![vec![serde_json::Value::String(text.to_string())]],
                    ),
                };

            Ok(QueryResult {
                columns,
                rows: parsed_rows.clone(),
                stats: QueryStats {
                    row_count: parsed_rows.len(),
                    execution_time_ms: execution_time,
                    scanned_rows: 0,
                },
            })
        },
        None => {
            let err_msg = resp.error.map(|e| e.message).unwrap_or("Query failed".to_string());
            Err(err_msg)
        }
    }
}

/// Get query explanation/plan
#[tauri::command]
pub async fn explain_query(
    state: State<'_, Arc<AppState>>,
    _query: String,
) -> Result<String, String> {
    let status = state.get_status().await;
    if !status.db_connected {
        return Err("No active connection".to_string());
    }
    
    // TODO: Implement explain via MCP
    Ok("Query plan: Full scan (explain not yet implemented)".to_string())
}
