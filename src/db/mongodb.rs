use crate::config::MongoDBConfig;
use crate::models::Dataset;
use anyhow::Result;
use futures::TryStreamExt;
use mongodb::bson::{DateTime, Document};
use mongodb::options::ReplaceOptions;
use mongodb::{
    bson::{doc, Regex},
    options::ClientOptions,
    Client, Collection, Database,
};
use tracing::info;

pub struct MongoDB {
    client: Client,
    database: Database,
}

impl MongoDB {
    pub async fn new(config: &MongoDBConfig) -> Result<Self> {
        let options = ClientOptions::parse(&config.uri).await?;
        let client = Client::with_options(options)?;
        let database = client.database(&config.database);
        info!("Connected to MongoDB");
        Ok(Self { client, database })
    }

    pub async fn upsert_dataset(&self, collection_name: &str, dataset: Dataset) -> Result<()> {
        let collection: Collection<Dataset> = self.database.collection(collection_name);

        let filter = doc! { "@id": &dataset.raw_id};

        let options = ReplaceOptions::builder().upsert(true).build();

        collection.replace_one(filter, dataset).with_options(options).await?;

        Ok(())
    }

    pub async fn get_datasets(&self, collection_name: &str) -> Result<Vec<Dataset>> {
        let collection: Collection<Dataset> = self.database.collection(collection_name);

        let filter = doc! {
            "@type": Regex {
                pattern: "Dataset".to_string(),
                options: "i".to_string(),
            }
        };

        let cursor = collection.find(filter).await?;
        let datasets = cursor.try_collect().await?;

        Ok(datasets)
    }

    pub async fn save_new_dataset_ids(&self, center_name: &str, new_ids: &[String]) -> Result<()> {
        let collection = self.database
            .collection::<Document>("processed_dataset_ids");
        if new_ids.is_empty() {
            return Ok(());
        }
        let documents: Vec<Document> = new_ids
            .iter()
            .map(|id| {
                doc! {
                "center_name": center_name,
                "dataset_id": id,
                "status": "pending",
                "created_at": DateTime::now()
            }
            })
            .collect();
        if !documents.is_empty() {
            collection.insert_many(documents).await?;
        }
        Ok(())
    }

    pub async fn check_existing_ids(&self, center_name: &str, ids: &[String]) -> Result<Vec<String>> {
        let filter = doc! {
            "center_name": center_name,
            "dataset_id": {"$in": ids}
        };

        let cursor = self.database.collection::<Document>("processed_dataset_ids").find(filter).await?;

        let existing_docs: Vec<Document> = cursor.try_collect().await?;

        let existing_ids: Vec<String> = existing_docs.into_iter()
            .filter_map(|doc| doc.get_str("dataset_id").ok().map(String::from))
            .collect();

        Ok(existing_ids)
    }

    // 获取已经处理过的ID列表
    pub async fn get_processed_ids(&self, center_name: &str) -> Result<Vec<String>> {
        let collection = self.database
            .collection::<Document>("processed_dataset_ids");

        let filter = doc! {
            "center_name": center_name,
            "status": "processed" // 只获取已处理的ID
        };

        let mut cursor = collection.find(filter).await?;
        let mut processed_ids = Vec::new();

        while let Some(doc) = cursor.try_next().await? {
            if let Some(dataset_id) = doc.get("dataset_id").and_then(|id| id.as_str()) {
                processed_ids.push(dataset_id.to_string());
            }
        }

        Ok(processed_ids)
    }

    // 更新ID状态为已处理
    pub async fn update_processed_ids(&self, center_name: &str, processed_ids: &[String]) -> Result<()> {
        if processed_ids.is_empty() {
            return Ok(());
        }
        let collection = self.database
            .collection::<Document>("processed_dataset_ids");
        let filter = doc! {
            "center_name": center_name,
            "dataset_id": {
                "$in": processed_ids
            }
        };
        let update = doc! {
            "$set": {
                "status": "processed",
                "processed_at": DateTime::now()
            }
        };
        collection.update_many(filter, update).await?;
        Ok(())
    }
}