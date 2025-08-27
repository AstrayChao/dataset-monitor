use crate::config::MongoDBConfig;
use crate::models::Dataset;
use anyhow::Result;
use futures::TryStreamExt;
use mongodb::options::ReplaceOptions;
use mongodb::{
    bson::{doc, Regex},
    options::ClientOptions,
    Client, Collection, Database,
};

pub struct MongoDB {
    client: Client,
    database: Database,
}

impl MongoDB {
    pub async fn new(config: &MongoDBConfig) -> Result<Self> {
        let options = ClientOptions::parse(&config.uri).await?;
        let client = Client::with_options(options)?;
        let database = client.database(&config.database);

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
}