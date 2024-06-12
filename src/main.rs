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

    let client = Client::new("http://localhost:7700", Option::<String>::None).unwrap();

    println!("Making all the indexes...");
    for index_uid in 0..8000 {
        let index = client.index(index_uid.to_string());
        tokio::task::spawn(handle_index(index));
    }

    let (mut last, mut last_finished) = last_task(&client).await;

    loop {
        let (current, current_finished) = last_task(&client).await;

        println!("Enqueued {} new tasks", current - last);
        println!("Processed {} new tasks", current_finished - last_finished);
        last = current;
        last_finished = current_finished;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn last_task(client: &Client) -> (u32, u32) {
    let last = TasksSearchQuery::new(client)
        .with_limit(1)
        .execute()
        .await
        .unwrap()
        .results
        .first()
        .map_or(0, |task| task.get_uid());
    let last_finished = TasksSearchQuery::new(client)
        .with_limit(1)
        .with_statuses(["succeeded", "failed", "canceled"])
        .execute()
        .await
        .unwrap()
        .results
        .first()
        .map_or(0, |task| task.get_uid());

    (last, last_finished)
}

async fn handle_index(index: Index) {
    // To reduce the number of requests at startup we introduce a random delay before the first request
    let delay: u64 = (1..30).fake();
    tokio::time::sleep(Duration::from_secs(delay)).await;
    assert!(index
        .set_settings(&Person::generate_settings())
        .await
        .unwrap()
        .wait_for_completion(&index.client, None, Some(Duration::MAX))
        .await
        .unwrap()
        .is_success());

    loop {
        let documents: Vec<Person> = fake::vec![Person; 50..1000];
        index.add_documents(&documents, Some("id")).await.unwrap();

        let delay: u64 = (1..30).fake();
        tokio::time::sleep(Duration::from_secs(delay)).await;
    }
}
