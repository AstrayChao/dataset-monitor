use anyhow::Result;
use clokwerk::{AsyncScheduler, TimeUnits};
use std::sync::Arc;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

use dataset_monitor::{config::Config, db, init_logging, DataMonitor};
async fn execute_url_monitoring(config: Arc<Config>) -> Result<()> {
    info!("开始执行URL监测任务");
    let monitor = DataMonitor::new(config);
    monitor.check_all_urls().await.map_err(|e| {
        error!("URL监测失败: {}", e);
        e
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    init_logging("data-monitor.log")?;

    info!("启动URL监测系统");

    // 加载配置
    let config = Config::load("config.yaml")?;
    let config_arc = Arc::new(config);

    db::init_duckdb(&config_arc.duckdb.path).await?;

    let scheduler = JobScheduler::new().await?;
    // 启动时立即执行一次
    if let Err(e) = execute_url_monitoring(config_arc.clone()).await {
        error!("首次URL监测失败: {}", e);
    }
    let check_interval_days = config_arc.monitor.check_interval_days;

    let cron_expression = format!("0 0 0 5/{} * *", check_interval_days);
    // URL监测任务
    let job = Job::new_async(&cron_expression, move |_uuid, _l| {
        let config = config_arc.clone();
        Box::pin(async move {
            if let Err(e) = execute_url_monitoring(config).await {
                error!("定时URL监测失败: {}", e);
            }
        })
    })?;

    scheduler.add(job).await?;
    scheduler.start().await?;

    // 保持程序运行
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
