use crate::config::Config;
use crate::db::mongodb::MongoDB;
use crate::models::{AuthResponse, Dataset};
use anyhow::{Context, Result};
use chrono::Utc;
use dashmap::DashMap;
use reqwest::header::{HeaderMap, HeaderValue};
use serde_json::Value;
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
                Ok(count) => info!("公司 {} 获取数据 {} 条", center.name, count),
                Err(e) => error!("公司 {} 获取失败: {}", center.name, e),
            }
        }
        Ok(())
    }

    async fn fetch_center_data(&self, name: &str, url: &str, secret_key: &str, db: &MongoDB) -> Result<usize> {
        let token_info = self.get_or_refresh_token(name, url, secret_key).await?;
        let dataset_list_url = token_info.services.iter()
            .find(|s| s.name == "DATASET_LIST")
            .context("未找到数据集服务")?
            .url.clone();
        let mut headers = HeaderMap::new();
        headers.insert("token", HeaderValue::from_str(&token_info.token)?);
        headers.insert("version", HeaderValue::from_str(&token_info.version)?);

        let response = self.client.get(&dataset_list_url)
            .headers(headers.clone())
            .send()
            .await?;

        let dataset_ids: Vec<String> = response.json::<Value>().await?
            .as_array()
            .context("响应不是数组")?
            .iter()
            .filter_map(|v| v.get("id").and_then(|id| id.as_str()).map(String::from))
            .collect();

        info!("获取到 {} 个数据集ID", dataset_ids.len());

        let details_url = token_info.services.iter()
            .find(|s| s.name == "GET_DATASET_DETAILS")
            .context("未找到GET_DATASET_DETAILS服务")?
            .url.clone();

        let mut count = 0;
        for id in dataset_ids {
            let response = self.client.get(&details_url)
                .headers(headers.clone())
                .query(&[("id", &id)])
                .send()
                .await?;

            if response.status().is_success() {
                let mut dataset: Dataset = response.json().await?;
                dataset.sync_date = Utc::now();
                dataset.center_name = name.to_string();
                db.upsert_dataset(name, dataset).await?;
                count += 1;
            }
        }
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