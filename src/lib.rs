pub mod config;
pub mod models;
pub mod db;
pub mod fetcher;
pub mod monitor;
pub mod api;

// 重新导出常用的类型和函数
pub use crate::config::Config;
pub use crate::fetcher::DataFetcher;
pub use crate::monitor::DataMonitor;

use anyhow::Result;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub fn init_logging(file_name: &str) -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    std::fs::create_dir_all("logs")?;

    let file_appender = tracing_appender::rolling::daily("logs", file_name);
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(file_appender);

    let (non_blocking_console, _console_guard) = tracing_appender::non_blocking(std::io::stderr());

    // 创建环境过滤器
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // 构建订阅者
    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .with_writer(non_blocking_console)
                .with_ansi(true)  // 彩色输出
                .with_target(true)
                .with_level(true)
                .with_line_number(true)
        )
        .with(
            fmt::layer()
                .with_writer(non_blocking_file)
                .with_ansi(false)  // 文件不使用彩色
                .with_target(true)
                .with_level(true)
                .with_line_number(true)
        )
        .init();

    // 将 _guard 存储在静态变量中以防止提前drop
    std::mem::forget(_guard);
    std::mem::forget(_console_guard);

    Ok(())
}