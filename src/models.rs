use chrono::{DateTime, Utc};
use mongodb::bson::oid::ObjectId;
use mongodb::bson::Bson;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct AuthResponse {
    pub ticket: Ticket,
    #[serde(rename = "serviceList")]
    pub service_list: Vec<Service>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticket {
    pub expires: i64,
    pub token: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Service {
    pub name: String,
    pub version: String,
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Dataset {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    pub _id: Option<ObjectId>,
    #[serde(rename = "@id")]
    pub raw_id: String,
    #[serde(rename = "@type")]
    pub data_type: Bson,
    #[serde(rename = "schema:url")]
    pub url: Option<Bson>,
    #[serde(rename = "schema:name")]
    pub name: Option<Bson>,
    #[serde(rename = "schema:datePublished")]
    pub date_published: Option<Bson>,
    #[serde(rename = "syncDate")]
    pub sync_date: DateTime<Utc>,
    #[serde(rename = "centerName")]
    pub center_name: String,
}

#[derive(Debug)]
pub struct MonitorRecord {
    pub id: String,
    pub raw_id: String,
    pub url: String,
    pub name: String,
    pub center_name: String,
    pub date_published: String,
    pub sync_date: DateTime<Utc>,
    pub check_time: DateTime<Utc>,
    pub status: i32,
    pub error_msg: Option<String>,
}

impl Dataset {
    pub fn extract_url(&self) -> Option<String> {
        match &self.url {
            Some(Bson::String(s)) => Some(s.clone()),
            _ => None,
        }
    }

    pub fn extract_name(&self) -> String {
        match &self.name {
            _ => "Unknown".to_string(),
            Some(Bson::String(s)) => s.clone(),
            Some(Bson::Document(doc)) => {
                doc.get("@value")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string()
            }
            Some(Bson::Array(arr)) => {
                arr.first()
                    .and_then(|item| match item {
                        Bson::String(s) => Some(s.clone()),
                        Bson::Document(d) => d.get("@value")
                            .and_then(|v| v.as_str().map(String::from)),
                        _ => None,
                    })
                    .unwrap_or_else(|| "unknown".to_string())
            }
        }
    }

    pub fn extract_date_published(&self) -> String {
        match &self.date_published {
            Some(Bson::String(s)) => s.clone(),
            Some(Bson::DateTime(dt)) => dt.to_string(),
            _ => "unknown".to_string(),
        }
    }
}