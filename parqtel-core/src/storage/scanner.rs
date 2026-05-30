use std::fs::File;
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel};
use crate::models::metrics::DataPoint;
use crate::models::logs::LogRecord;
use arrow2::io::parquet::read;

/// Reads and filters data points from Parquet files.
pub struct Scanner;

impl Scanner {
    /// Scans a set of blocks for data points matching a metric and time range.
    pub async fn scan(
        blocks: Vec<BlockMetadata>,
        metric_name: String,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<DataPoint>> {
        let mut tasks = Vec::new();
        for block in blocks {
            let m_name = metric_name.clone();
            tasks.push(tokio::spawn(async move {
                Self::scan_block(block, m_name, start_ns, end_ns)
            }));
        }

        let mut all_points = Vec::new();
        for task in tasks {
            let points = task.await.map_err(|e| Error::Internal(e.to_string()))??;
            all_points.extend(points);
        }
        all_points.sort_by_key(|p| p.timestamp_ns);
        Ok(all_points)
    }

    fn scan_block(meta: BlockMetadata, metric_name: String, start_ns: i64, end_ns: i64) -> Result<Vec<DataPoint>> {
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = StorageModel::metrics_schema();
        let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

        let mut points = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            for row in 0..chunk.len() {
                let (name, _kind, _res, dp) = StorageModel::row_to_point(&chunk, row)?;
                let name_match = metric_name.is_empty() || name == metric_name;
                if name_match && dp.timestamp_ns >= start_ns && dp.timestamp_ns <= end_ns {
                    points.push(dp);
                }
            }
        }
        Ok(points)
    }

    /// Scans a set of blocks for log records matching a time range.
    pub async fn scan_logs(
        blocks: Vec<BlockMetadata>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<LogRecord>> {
        let mut tasks = Vec::new();
        for block in blocks {
            tasks.push(tokio::spawn(async move {
                Self::scan_log_block(block, start_ns, end_ns)
            }));
        }

        let mut all_logs = Vec::new();
        for task in tasks {
            let logs = task.await.map_err(|e| Error::Internal(e.to_string()))??;
            all_logs.extend(logs);
        }
        all_logs.sort_by_key(|l| l.timestamp_ns);
        Ok(all_logs)
    }

    fn scan_log_block(meta: BlockMetadata, start_ns: i64, end_ns: i64) -> Result<Vec<LogRecord>> {
        let mut file = match File::open(&meta.path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("Log block file not found, skipping: {:?}", meta.path);
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
        let schema = StorageModel::logs_schema();
        let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

        let mut logs = Vec::new();
        for chunk in reader {
            let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
            for row in 0..chunk.len() {
                let log = StorageModel::row_to_log(&chunk, row)?;
                if log.timestamp_ns >= start_ns && log.timestamp_ns <= end_ns {
                    logs.push(log);
                }
            }
        }
        Ok(logs)
    }
}
