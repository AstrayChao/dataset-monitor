use anyhow::Result;
use clokwerk::{AsyncScheduler, TimeUnits};
use mongodb::bson::{doc, Document};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

mod config;
mod models;
mod db;
mod fetcher;
mod monitor;

use crate::config::MongoDBConfig;
use crate::db::mongodb::MongoDB;
use config::Config;
use fetcher::DataFetcher;
use monitor::DataMonitor;

#[tokio::main]
async fn main() -> Result<()> {
    init_logging()?;

    info!("启动URL监测系统");

    // 加载配置
    let config = Config::load("config.yaml")?;
    let config_arc = std::sync::Arc::new(config);

    // 初始化数据库
    db::init_duckdb(&config_arc.duckdb.path).await?;
    let fetcher = DataFetcher::new(config_arc.clone());
    if let Err(e) = fetcher.fetch_all_center().await {
        error!("数据获取失败: {}", e);
    }    // 创建调度器
    let mut scheduler = AsyncScheduler::new();

    let config_clone = config_arc.clone();
    // 数据获取任务 - 每月执行
    scheduler_data_fetch(&config_arc, &mut scheduler, config_clone);


    // let config_clone = config_arc.clone();
    // // URL监测任务 - 每周执行
    // schedulerDataMonitor(config_arc, &mut scheduler, config_clone);

    // 运行调度器
    loop {
        scheduler.run_pending().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}


fn init_logging() -> Result<()> {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};
    std::fs::create_dir_all("logs")?;

    let file_appender = tracing_appender::rolling::daily("logs", "dataset-monitor.log");
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

fn scheduler_data_fetch(config_arc: &Arc<Config>, scheduler: &mut AsyncScheduler, config_clone: Arc<Config>) {
    scheduler.every(config_arc.monitor.fetch_interval_days.days()).run(move || {
        let config = config_clone.clone();
        async move {
            info!("开始执行数据获取任务");
            let fetcher = DataFetcher::new(config);
            if let Err(e) = fetcher.fetch_all_center().await {
                error!("数据获取失败: {}", e);
            }
        }
    });
}

fn scheduler_data_monitor(config_arc: Arc<Config>, scheduler: &mut AsyncScheduler, config_clone: Arc<Config>) {
    scheduler.every(config_arc.monitor.check_interval_days.days()).run(move || {
        let config = config_clone.clone();
        async move {
            info!("开始执行URL监测任务");
            let monitor = DataMonitor::new(config);
            if let Err(e) = monitor.check_all_urls().await {
                error!("URL监测失败: {}", e);
            }
        }
    });
}