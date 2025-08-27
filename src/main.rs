use anyhow::Result;
use clokwerk::{AsyncScheduler, TimeUnits};
use std::time::Duration;
use tracing::{error, info};

mod config;
mod models;
mod db;
mod fetcher;
mod monitor;

use config::Config;
use fetcher::DataFetcher;
use monitor::DataMonitor;

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter("url_monitor=info")
        .init();

    info!("启动URL监测系统");

    // 加载配置
    let config = Config::load("config.yaml")?;
    let config_arc = std::sync::Arc::new(config);

    // 初始化数据库
    db::init_duckdb(&config_arc.duckdb.path).await?;

    // 创建调度器
    let mut scheduler = AsyncScheduler::new();

    let config_clone = config_arc.clone();
    // 数据获取任务 - 每月执行
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

    let config_clone = config_arc.clone();
    // URL监测任务 - 每周执行
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

    // 立即执行一次
    info!("执行初始任务");
    let fetcher = DataFetcher::new(config_arc.clone());
    fetcher.fetch_all_center().await?;

    let monitor = DataMonitor::new(config_arc.clone());
    monitor.check_all_urls().await?;

    // 运行调度器
    loop {
        scheduler.run_pending().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}