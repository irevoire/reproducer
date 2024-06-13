use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use fake::faker::address::fr_fr::{Latitude, Longitude};
use fake::faker::administrative::fr_fr::HealthInsuranceCode;
use fake::faker::automotive::fr_fr::LicencePlate;
use fake::faker::creditcard::fr_fr::CreditCardNumber;
use fake::faker::lorem::fr_fr::{Paragraph, Words};
use fake::faker::name::fr_fr::Name;
use fake::faker::time::fr_fr::Date;
use fake::uuid::UUIDv4;
use fake::{Dummy, Fake, Faker};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::documents::IndexConfig;
use meilisearch_sdk::indexes::Index;
use meilisearch_sdk::tasks::TasksSearchQuery;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Serialize, Deserialize, IndexConfig, Dummy, Debug)]
struct Person {
    #[index_config(primary_key)]
    id: u16,
    #[index_config(displayed, searchable)]
    #[dummy(faker = "Name()")]
    name: String,
    #[index_config(displayed, searchable)]
    #[dummy(faker = "Paragraph(50..500)")]
    description: String,
    #[index_config(filterable, sortable, displayed)]
    #[dummy(faker = "Date()")]
    born: String,
    #[index_config(filterable, displayed)]
    #[dummy(faker = "Words(10..50)")]
    label: Vec<String>,
    // We make it searchable intentionally to make meilisearch struggles
    #[index_config(searchable, filterable, displayed)]
    #[dummy(faker = "UUIDv4")]
    #[serde(with = "uuid::serde::braced")]
    identifier: Uuid,
    // same
    #[index_config(searchable, filterable, displayed)]
    #[dummy(faker = "CreditCardNumber()")]
    credit_card: String,
    // same
    #[index_config(searchable, filterable, displayed)]
    #[dummy(faker = "HealthInsuranceCode()")]
    secu: String,
    #[index_config(searchable, filterable, displayed)]
    #[dummy(faker = "LicencePlate()")]
    licence_plate: String,

    #[index_config(filterable, sortable, displayed)]
    _geo: Geo,
}

#[derive(Serialize, Deserialize, IndexConfig, Dummy, Debug)]
struct Geo {
    #[dummy(faker = "Latitude()")]
    lat: f64,
    #[dummy(faker = "Longitude()")]
    lng: f64,
}

static NB_INDEXES_READY: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() {
    println!(
        "Sending this kind of documents:\n{}",
        serde_json::to_string_pretty(&Faker.fake::<Person>()).unwrap()
    );
    println!(
        "With these settings:\n{}",
        serde_json::to_string_pretty(&Person::generate_settings()).unwrap()
    );
    let send_settings = std::env::args().nth(1).is_some();
    if !send_settings {
        println!("Skipping the initialization of the indexes");
    }

    let client = Client::new("http://localhost:7700", Option::<String>::None).unwrap();

    let channel_capacity = 10;
    let (sender, receiver) = broadcast::channel(channel_capacity);
    // Fill the broadcast channel
    for _ in 0..channel_capacity {
        let documents: Vec<Person> = fake::vec![Person; 50..100];
        sender.send(Arc::new(documents)).unwrap();
    }

    let (mut last, mut last_finished) = last_task(&client).await.unwrap();

    let nb_indexes = 8000;
    println!("Making all the indexes...");
    for index_uid in 0..nb_indexes {
        let index = client.index(index_uid.to_string());
        tokio::task::spawn(handle_index(index, send_settings, receiver.resubscribe()));
    }
    drop(receiver);

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        match last_task(&client).await {
            Ok((current, current_finished)) => {
                println!(
                    "Indexes ready to send: {}/{}",
                    NB_INDEXES_READY.load(Ordering::Relaxed),
                    nb_indexes
                );
                println!("Enqueued {} new tasks", current - last);
                println!("Processed {} new tasks", current_finished - last_finished);
                println!();
                last = current;
                last_finished = current_finished;
            }
            Err(e) => {
                println!("ERROR: {e}");
            }
        }
        let documents: Vec<Person> = fake::vec![Person; 50..1000];
        sender.send(Arc::new(documents)).unwrap();
    }
}

async fn last_task(client: &Client) -> Result<(u32, u32), meilisearch_sdk::errors::Error> {
    let last = TasksSearchQuery::new(client)
        .with_limit(1)
        .execute()
        .await?
        .results
        .first()
        .map_or(0, |task| task.get_uid());
    let last_finished = TasksSearchQuery::new(client)
        .with_limit(1)
        .with_statuses(["succeeded", "failed", "canceled"])
        .execute()
        .await?
        .results
        .first()
        .map_or(0, |task| task.get_uid());

    Ok((last, last_finished))
}

async fn handle_index(
    index: Index,
    settings: bool,
    mut receiver: broadcast::Receiver<Arc<Vec<Person>>>,
) {
    // To reduce the number of requests at startup we introduce a random delay before the first request
    if settings {
        loop {
            let delay: u64 = (1..30).fake();
            tokio::time::sleep(Duration::from_secs(delay)).await;
            match send_settings(&index).await {
                Ok(()) => break,
                Err(e) => println!("ERROR WHILE SENDING SETTINGS: {e}"),
            }
        }
    }

    NB_INDEXES_READY.fetch_add(1, Ordering::Relaxed);

    loop {
        match receiver.recv().await {
            Ok(documents) => {
                let _ = index.add_documents(&documents, Some("id")).await;
            }
            // We don't care about the lost message
            Err(broadcast::error::RecvError::Lagged(_)) => continue,
            Err(broadcast::error::RecvError::Closed) => break,
        }

        let delay: u64 = (30..60).fake();
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}

async fn send_settings(index: &Index) -> Result<(), meilisearch_sdk::errors::Error> {
    let delay: u64 = (30..60).fake();
    let ret = index
        .set_settings(&Person::generate_settings())
        .await?
        .wait_for_completion(
            &index.client,
            // If we poll too often we'll exhaust the available port quite fast
            Some(Duration::from_secs(delay)),
            Some(Duration::MAX),
        )
        .await?;
    if ret.is_failure() {
        Err(ret.unwrap_failure().into())
    } else {
        Ok(())
    }
}
