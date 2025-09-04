use anyhow::Result;
use clokwerk::{AsyncScheduler, TimeUnits};
use dataset_monitor::{config::Config, db, init_logging, DataMonitor};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("data-monitor.log")?;

    info!("启动URL监测系统");

    // 加载配置
    let config = Config::load("config.yaml")?;
    let config_arc = Arc::new(config);

    db::init_duckdb(&config_arc.duckdb.path).await?;

    let mut scheduler = AsyncScheduler::new();

    // URL监测任务
    let config_clone = config_arc.clone();
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

    loop {
        scheduler.run_pending().await;
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
