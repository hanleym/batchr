mod parse;

use std::io::SeekFrom;
use std::sync::Arc;
use anyhow::{Context, Result};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tokio::fs::File;
use futures::{stream, StreamExt};
use tokio::io::AsyncSeekExt;
use tokio::sync::Mutex;
use crate::parse::Statement;

const BATCH_TARGET_BYTES: usize = 2000000;

#[derive(Debug, Clone)]
enum Section {
    Definitions(String),
    Data(String),
}

impl std::fmt::Display for Section {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (kind, table) = match self {
            Section::Definitions(table) => ("definitions", table),
            Section::Data(table) => ("data", table),
        };
        f.write_fmt(format_args!("{} {}", table, kind))
    }
}

struct Table {
    name: String,
    offset: u64,
    statements: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = DB::new(
        &std::env::var("SURREALDB_ENDPOINT")?,
        &std::env::var("SURREALDB_USERNAME")?,
        &std::env::var("SURREALDB_PASSWORD")?,
        &std::env::var("SURREALDB_NAMESPACE")?,
        &std::env::var("SURREALDB_DATABASE")?,
    );

    println!("Removing namespace: {}", &db.namespace);
    db.sql(&format!(
        "REMOVE NAMESPACE IF EXISTS {};",
        &db.namespace,
    )).await?;
    println!("Removed namespace: {}", &db.namespace);

    let filepath = std::env::args().nth(1).context("No file path provided")?;

    let file = File::open(&filepath).await.context("Failed to open file")?;
    let mut stream = parse::StatementStream::new(file);

    let mut definitions = Vec::new();
    let mut tables = Vec::new();
    let mut data_offset = 0;
    let mut data_statements = 0;

    let mut section = Section::Definitions("Initial".to_string());

    while let Some(result) = stream.next_statement().await {
        let (offset, result) = result.context("Failed to parse next statement")?;
        match result {
            Statement::Comment(comment) => {
                let comment = comment.trim_matches(|c| c == '-' || c == ' ');
                let (prefix, suffix) = if let Some(index) = comment.find(": ") {
                    comment.split_at(index)
                } else {
                    (comment, "")
                };

                let prefix = prefix.trim_matches(|c| c == ':' || c == ' ');
                let suffix = suffix.trim_matches(|c| c == ':' || c == ' ');

                match prefix {
                    "" => {
                        continue;
                    },
                    // "OPTION" | "ACCESSES" | "FUNCTIONS" => {
                    //     continue;
                    // }
                    "TABLE" => {
                        println!("STARTING TABLE: {}, PREVIOUS: {}", suffix, section);
                        match section {
                            Section::Definitions(_) => {}
                            Section::Data(table) => {
                                tables.push(Table {
                                    name: table,
                                    offset: data_offset,
                                    statements: data_statements,
                                });
                                data_statements = 0;
                            }
                        }
                        section = Section::Definitions(suffix.to_string());
                    }
                    "TABLE DATA" => {
                        section = Section::Data(suffix.to_string());
                        data_offset = offset;
                    }
                    _ => println!("-- {}", comment),
                }
            }
            Statement::Query(query) => {
                match section {
                    Section::Definitions(_) => {
                        definitions.push(query);
                    },
                    Section::Data(_) => {
                        data_statements += 1;
                    }
                }
            }
        }
    }

    // finish if we end on a data section
    match section {
        Section::Definitions(_) => {}
        Section::Data(table) => {
            tables.push(Table {
                name: table,
                offset: data_offset,
                statements: data_statements,
            });
            data_statements = 0;
        }
    }

    // dbg!(&definitions);

    println!("Planning to import {} definition statements.", definitions.len());
    db.import("Initial", 0, &definitions).await.context("Failed to import definitions")?;
    println!("Successfully imported {} definition statements.", definitions.len());

    println!("Planning to import {} tables.", tables.len());

    let bars = Arc::new(Mutex::new(MultiProgress::new()));
    let style = ProgressStyle::with_template(
        "{msg}\n⤷[{elapsed_precise}] [{wide_bar}] {human_pos}/{human_len} [{eta_precise}]",
    )?
        .progress_chars("##-");

    stream::iter(tables.iter())
        .map(|table| {
            let filepath = filepath.clone();
            let db = db.clone();
            let bars = bars.clone();
            let style = style.clone();
            async move {
                let progress = {
                    let bars = bars.lock().await;
                    let bar = bars.add(ProgressBar::new(table.statements));
                    drop(bars);
                    bar
                };
                progress.set_style(style.clone());
                progress.set_position(0);

                let mut file = File::open(&filepath).await.unwrap();
                file.seek(SeekFrom::Start(table.offset)).await.unwrap();
                let mut stream = parse::StatementStream::new(file);
                let mut completed = 0;

                let mut batch = Vec::new();
                let mut bytes = 0;
                while completed < table.statements {
                    while let Some(result) = stream.next_statement().await {
                        let (_, result) = result.context("Failed to parse next statement").unwrap();
                        match result {
                            Statement::Comment(comment) => {
                                let comment = comment.trim_matches(|c| c == '-' || c == ' ');
                                let (prefix, _suffix) = if let Some(index) = comment.find(": ") {
                                    comment.split_at(index)
                                } else {
                                    (comment, "")
                                };

                                let prefix = prefix.trim_matches(|c| c == ':' || c == ' ');
                                // let suffix = suffix.trim_matches(|c| c == ':' || c == ' ');

                                match prefix {
                                    "" => {
                                        continue;
                                    },
                                    // "OPTION" | "ACCESSES" | "FUNCTIONS" => {
                                    //     continue;
                                    // }
                                    "TABLE" => {
                                        {
                                            let bars = bars.lock().await;
                                            bars.remove(&progress);
                                            drop(bars);
                                        }
                                        break;
                                    }
                                    "TABLE DATA" => {
                                        continue;
                                    }
                                    _ => progress.set_message(format!("-- {}", comment)),
                                }
                            }
                            Statement::Query(query) => {
                                let len = query.len();
                                bytes += len;
                                batch.push(query);
                                if bytes >= BATCH_TARGET_BYTES {
                                    break;
                                }
                            }
                        }
                    }

                    progress.set_message(format!(
                        "Importing {} data with {} statements and {} bytes:",
                        table.name, batch.len(), bytes
                    ));

                    if let Err(err) = db.import(&table.name, completed, &batch).await {
                        progress.set_message(format!("Failed to import {}: {}", table.name, err.to_string()));
                    }

                    completed += batch.len() as u64;
                    progress.set_position(completed);
                    batch.clear();
                    bytes = 0;
                }
            }
        })
        .buffer_unordered(10)
        .collect::<Vec<_>>()
        .await;

    println!("Successfully imported {} tables.", tables.len());

    Ok(())
}

#[derive(Clone)]
struct DB {
    http: reqwest::Client,
    endpoint: String,
    username: String,
    password: String,
    namespace: String,
    database: String,
}

impl DB {
    fn new(endpoint: &str, username: &str, password: &str, namespace: &str, database: &str) -> Self {
        Self {
            http: reqwest::Client::new(),
            endpoint: endpoint.to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
            namespace: namespace.to_owned(),
            database: database.to_owned(),
        }
    }

    async fn import(&self, table: &str, completed: u64, batch: &Vec<String>) -> Result<()> {
        let sql = format!(
            "BEGIN TRANSACTION;\nOPTION IMPORT;\n{}\nCOMMIT TRANSACTION;",
            batch.join("\n")
        );

        let res = self.http
            .post(format!("{}/import", self.endpoint))
            .header("Accept", "application/json")
            .header("Surreal-NS", &self.namespace)
            .header("Surreal-DB", &self.database)
            .basic_auth(&self.username, Some(&self.password))
            .body(sql)
            .send()
            .await?;

        if !res.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to run import query; error: {}\n{}",
                res.status(),
                res.text().await?,
            ));
        }

        let results = res.json::<Vec<serde_json::Value>>().await?;
        let mut errors = Vec::new();
        for result in results {
            let status = result.get("status").context("Failed to parse result: no 'status' field")?;
            let status = status.as_str().context("Failed to parse result: 'status' field is not a string")?;
            if status == "ERR" {
                errors.push(
                    result.get("result").context("Failed to parse result: no 'result' field")?
                        .as_str().context("Failed to parse result: 'result' field is not a string")?
                        .to_owned()
                );
            }
        }

        if !errors.is_empty() {
            let dump = std::fs::File::create(format!("{}-Errors.json", table))?;
            serde_json::to_writer_pretty(&dump, &DumpFile{
                errors: errors.clone(),
                queries: batch.clone(),
            })?;
            dump.sync_all()?;
            drop(dump);

            for (index, err) in errors.iter().enumerate() {
                if !err.contains("not executed due to a failed transaction") {
                    return Err(anyhow::anyhow!(
                        "Error at index {}",
                        index as u64 + completed,
                    ));
                }
                return Err(anyhow::anyhow!("Error at unknown location."));
            }
        }

        Ok(())
    }

    async fn sql(&self, sql: &str) -> Result<()> {
        let res = self.http
            .post(format!("{}/sql", self.endpoint))
            .header("Accept", "application/json")
            .header("Surreal-NS", &self.namespace)
            .header("Surreal-DB", &self.database)
            .basic_auth(&self.username, Some(&self.password))
            .body(sql.to_owned())
            .send()
            .await?;

        if !res.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to run sql query; error: {}\n{}\nSQL:{}",
                res.status(),
                res.text().await?,
                sql,
            ));
        }

        let results = res.json::<Vec<serde_json::Value>>().await?;
        let mut errors = Vec::new();
        for result in results {
            let status = result.get("status").context("Failed to parse result: no 'status' field")?;
            let status = status.as_str().context("Failed to parse result: 'status' field is not a string")?;
            if status == "ERR" {
                errors.push(
                    result.get("result").context("Failed to parse result: no 'result' field")?
                        .as_str().context("Failed to parse result: 'result' field is not a string")?
                        .to_owned()
                );
            }
        }

        if !errors.is_empty() {
            let s = format!("Import errors:\n{}\n", errors.join("\n"));
            let s2 = format!("SQL:\n{}\n", sql);
            return Err(anyhow::anyhow!(s + &s2));
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct DumpFile {
    errors: Vec<String>,
    queries: Vec<String>,
}
