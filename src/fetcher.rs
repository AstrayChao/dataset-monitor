use crate::config::Config;
use crate::db::mongodb::MongoDB;
use crate::models::{AuthResponse, Dataset};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub struct DataFetcher {
    config: Arc<Config>,
    client: reqwest::Client,
    tokens: Arc<DashMap<String, TokenInfo>>,
}

struct TokenInfo {
    token: String,
    version: String,
    services: Vec<ServiceInfo>,
    expires_at: chrono::DateTime<Utc>,
}

struct ServiceInfo {
    name: String,
    url: String,
}

impl DataFetcher {
    pub fn new(config: Arc<Config>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.monitor.http_timeout_secs))
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true)
            .build()
            .expect("failed to build http client");
        Self {
            config,
            client,
            tokens: Arc::new(DashMap::new()),
        }
    }

    pub async fn fetch_all_center(&self) -> Result<()> {
        let db = MongoDB::new(&self.config.mongodb).await?;
        for center in &self.config.centers {
            info!("开始获取数据中心 {} 的数据", center.name);

            match self.fetch_center_data(&center.name, &center.url, &center.secret_key, &db).await {
                Ok(count) => info!("中心 {} 获取数据 {} 条", center.name, count),
                Err(e) => error!("公司 {} 获取失败: {:#?}\nBacktrace: {:?}", center.name, e, e.backtrace())
            }
        }
        Ok(())
    }

    async fn fetch_center_data(&self, name: &str, url: &str, secret_key: &str, db: &MongoDB) -> Result<usize> {
        let discovered = self.discover_new_ids(name, url, secret_key, db).await?;
        info!("数据中心 {} 本次发现新数据 {} 条", name, discovered);

        // 获取库中 状态为还未处理的数据集(刚发现+历史遗留的)
        let processed = self.process_pending_datasets(name, url, secret_key, db).await?;
        info!("数据中心 {} 本次处理数据 {} 条", name, processed);

        Ok(processed)
    }

    async fn discover_new_ids(&self, name: &str, url: &str, secret_key: &str, db: &MongoDB) -> Result<usize> {
        let token_info = self.get_or_refresh_token(name, url, secret_key).await?;
        let dataset_list_url = token_info.services.iter()
            .find(|s| s.name == "DATASET_LIST")
            .context("未找到数据集服务")?
            .url.clone();

        let mut headers = HeaderMap::new();
        headers.insert("token", HeaderValue::from_str(&token_info.token)?);
        headers.insert("version", HeaderValue::from_str(&token_info.version)?);

        // 请求数据集 ID 列表
        let response = self.client.get(&dataset_list_url)
            .headers(headers)
            .send()
            .await
            .with_context(|| format!("{} 获取数据集列表失败", name))?;

        let all_dataset_ids: Vec<String> = response.json::<Value>().await?
            .as_array()
            .with_context(|| format!("{} 响应不是数组", name))?
            .iter()
            .filter_map(|v| v.get("id").and_then(|id| id.as_str()).map(String::from))
            .collect();

        // DB 已有的 ID（不管是否 processed）
        let existing_ids: HashSet<String> = db.get_dataset_by_center(name).await?
            .into_iter()
            .collect();

        // 过滤掉 DB 已有的，剩下的才是全新 ID
        let new_ids: Vec<String> = all_dataset_ids
            .into_iter()
            .filter(|id| !existing_ids.contains(id))
            .collect();

        if new_ids.is_empty() {
            info!("{} 没有新 ID", name);
            return Ok(0);
        }

        // 保存为未处理状态
        db.save_new_dataset_ids(name, &new_ids).await
            .with_context(|| format!("{} 保存数据集ID失败", name))?;

        info!("{} 发现并保存了 {} 个新 ID", name, new_ids.len());
        Ok(new_ids.len())
    }

    async fn process_pending_datasets(&self, name: &str, url: &str, secret_key: &str, db: &MongoDB) -> Result<usize> {
        let token_info = self.get_or_refresh_token(name, url, secret_key).await?;
        let details_url = token_info.services.iter()
            .find(|s| s.name == "GET_DATASET_DETAILS")
            .with_context(|| format!("{} 未找到GET_DATASET_DETAILS服务", name))?
            .url.clone();
        let mut headers = HeaderMap::new();
        headers.insert("token", HeaderValue::from_str(&token_info.token)?);
        headers.insert("version", HeaderValue::from_str(&token_info.version)?);

        // 查找所有未处理的 ID
        let pending_ids = db.get_unprocessed_ids(name).await?;
        if pending_ids.is_empty() {
            info!("{} 没有待处理的 ID", name);
            return Ok(0);
        }

        let mut count = 0;
        for id in pending_ids {
            let response = self.client.get(&details_url)
                .headers(headers.clone())
                .query(&[("id", &id)])
                .send()
                .await
                .with_context(|| format!("{} 获取数据集 {} 详情失败", name, id))?;

            if response.status().is_success() {
                let mut dataset: Dataset = response.json().await
                    .with_context(|| format!("{} 解析数据集 {} 详情失败", name, id))?;
                dataset.casdc_id = Some(id.clone());
                db.upsert_dataset(name, dataset).await
                    .with_context(|| format!("{} 保存数据集 {} 失败", name, id))?;
                db.update_processed_ids(name, &[id.clone()]).await?;
                count += 1;
            } else {
                error!("{} 获取数据集 {} 详情失败，HTTP状态码: {}", name, id, response.status());
            }
        }

        info!("{} 成功处理 {} 个数据集详情", name, count);
        Ok(count)
    }

    async fn get_or_refresh_token(&self, name: &str, url: &str, key: &str) -> Result<TokenInfo> {
        // 检查缓存
        if let Some(token_info) = self.tokens.get(name) {
            if token_info.expires_at > Utc::now() {
                return Ok(token_info.clone());
            }
        }

        // 获取新token
        info!("获取公司 {} 的新token", name);

        let mut headers = HeaderMap::new();
        headers.insert("secretKey", HeaderValue::from_str(key)?);

        let response = self.client.get(url)
            .headers(headers)
            .send()
            .await
            .context("请求token失败")?;

        let auth_resp: AuthResponse = response.json().await?;

        let token_info = TokenInfo {
            token: auth_resp.ticket.token,
            version: auth_resp.service_list.first()
                .map(|s| s.version.clone())
                .unwrap_or_else(|| "1.0".to_string()),
            services: auth_resp.service_list.iter()
                .map(|s| ServiceInfo {
                    name: s.name.clone(),
                    url: s.url.clone(),
                })
                .collect(),
            expires_at: Utc::now() + chrono::Duration::seconds(auth_resp.ticket.expires - 300),
        };

        self.tokens.insert(name.to_string(), token_info.clone());
        Ok(token_info)
    }
}

impl Clone for TokenInfo {
    fn clone(&self) -> Self {
        Self {
            token: self.token.clone(),
            version: self.version.clone(),
            services: self.services.clone(),
            expires_at: self.expires_at,
        }
    }
}

impl Clone for ServiceInfo {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            url: self.url.clone(),
        }
    }
}