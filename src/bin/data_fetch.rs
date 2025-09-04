use anyhow::Result;
use dataset_monitor::db::mongodb::MongoDB;
use dataset_monitor::{config, db, init_logging, DataFetcher};
use std::sync::Arc;
use std::time::Duration;
use tokio_cron_scheduler::{Job, JobScheduler};
use tracing::{error, info};

async fn execute_data_fetch(config: Arc<config::Config>, db: Arc<MongoDB>) -> Result<()> {
    info!("开始执行数据获取任务");
    let fetcher = DataFetcher::new(config);
    fetcher.fetch_all_center(&db).await.map_err(|e| {
        error!("数据获取失败: {}", e);
        e
    })
}
#[tokio::main]
async fn main() -> Result<()> {
    init_logging("data-fetch.log")?;

    info!("启动数据获取系统");

    // 加载配置
    let config = config::Config::load("config.yaml")?;
    let config_arc = Arc::new(config);

    let db = Arc::new(MongoDB::new(&config_arc.mongodb).await?);
    db::init_duckdb(&config_arc.duckdb.path).await?;
    let scheduler = JobScheduler::new().await?;
    if let Err(e) = execute_data_fetch(config_arc.clone(), db.clone()).await {
        error!("首次数据获取失败: {}", e);
    }

    let fetch_interval_days = config_arc.monitor.fetch_interval_days;
    let cron_expression = format!("0 0 0 */{} * *", fetch_interval_days);

    let job = Job::new_async(&cron_expression, move |_uuid, _l| {
        let config = config_arc.clone();
        let db = db.clone(); // 这里 clone Arc，而不是 MongoDB 本身
        Box::pin(async move {
            if let Err(e) = execute_data_fetch(config, db).await {
                error!("定时数据获取失败: {}", e);
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