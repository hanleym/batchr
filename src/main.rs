mod parse;

use std::fmt::Formatter;
use anyhow::{Context, Result};
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{Seek, SeekFrom};
use crate::parse::Statement;

const BATCH_TARGET_BYTES: usize = 1000000;

#[derive(Debug, Clone)]
enum Section {
    Definitions(String),
    Data(String),
}

impl std::fmt::Display for Section {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let (kind, table) = match self {
            Section::Definitions(table) => ("definitions", table),
            Section::Data(table) => ("data", table),
        };
        f.write_fmt(format_args!("{} {}", table, kind))
    }
}

fn main() -> Result<()> {
    let db = DB::new(
        &std::env::var("SURREALDB_ENDPOINT")?,
        &std::env::var("SURREALDB_USERNAME")?,
        &std::env::var("SURREALDB_PASSWORD")?,
        &std::env::var("SURREALDB_NAMESPACE")?,
        &std::env::var("SURREALDB_DATABASE")?,
    );

    let filepath = std::env::args().nth(1).context("No file path provided")?;
    let start = std::env::args().nth(2).map(|s| s.parse::<u64>()).transpose()?;

    let file = File::open(&filepath).context("Failed to open file")?;
    let mut stream = parse::StatementStream::new(file);

    let mut total = 0;
    while let Some(result) = stream.next_statement() {
        match result.context("Failed to parse statement statement")? {
            Statement::Query(_) => total += 1,
            Statement::Comment(_) => {}
        }
    }
    let mut file = stream.into_inner();
    file.seek(SeekFrom::Start(0))?;
    let mut stream = parse::StatementStream::new(file);
    // let total = 15414;

    println!("Planning to import {} statements.", total);

    if start.is_none() {
        println!("Start is not set; removing namespace: {}", &db.namespace);
        db.sql(&format!(
            "REMOVE NAMESPACE IF EXISTS {};",
            &db.namespace,
        ))?;
        println!("Removed namespace: {}", &db.namespace);
    }

    let progress = ProgressBar::new(total);
    progress.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] [{wide_bar}] {human_pos}/{human_len} [{eta_precise}]\n{msg}")?
        .progress_chars("#>-")
    );
    progress.set_position(0);

    let mut completed = 0;
    if let Some(start) = start {
        println!("Resuming at: {}", start);
        while let Some(result) = stream.next_statement() {
            result.context("Failed to parse next statement")?;
            completed += 1;
            progress.set_position(completed);
            if completed >= start {
                break;
            }
        }
    }

    let mut next = Section::Definitions("Initial".to_string());
    let mut batch = Vec::new();
    let mut bytes = 0;
    while completed < total {
        let section = next.clone();

        while let Some(result) = stream.next_statement() {
            match result.context("Failed to parse next statement")? {
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
                            next = Section::Definitions(suffix.to_string());
                            break;
                        }
                        "TABLE DATA" => {
                            next = Section::Data(suffix.to_string());
                            break;
                        }
                        _ => progress.set_message(format!("-- {}", comment)),
                    }
                }
                Statement::Query(query) => {
                    let len = query.len();
                    bytes += len;

                    batch.push(query);

                    if matches!(section, Section::Data(_)) {
                        if bytes >= BATCH_TARGET_BYTES {
                            break;
                        }
                    }
                }
            }
        }

        progress.set_message(format!("Importing {} with {} statements and {} bytes.",
            section, batch.len(), bytes
        ));

        db.import(&batch).with_context(|| format!("Failed to import at: {}", completed))?;

        completed += batch.len() as u64;
        progress.set_position(completed);
        batch.clear();
        bytes = 0;
    }

    progress.finish();
    println!("Imported completed {} statements.", completed);

    Ok(())
}

struct DB {
    http: reqwest::blocking::Client,
    endpoint: String,
    username: String,
    password: String,
    namespace: String,
    database: String,
}

impl DB {
    fn new(endpoint: &str, username: &str, password: &str, namespace: &str, database: &str) -> Self {
        Self {
            http: reqwest::blocking::Client::new(),
            endpoint: endpoint.to_owned(),
            username: username.to_owned(),
            password: password.to_owned(),
            namespace: namespace.to_owned(),
            database: database.to_owned(),
        }
    }

    fn import(&self, batch: &Vec<String>) -> Result<()> {
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
            .send()?;

        if !res.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to run import query; error: {}\n{}",
                res.status(),
                res.text()?,
            ));
        }

        let results = res.json::<Vec<serde_json::Value>>()?;
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
            let dump = std::fs::File::create("data/dump.json")?;
            serde_json::to_writer_pretty(&dump, &DumpFile{
                errors: errors.clone(),
                queries: batch.clone(),
            })?;
            dump.sync_all()?;
            drop(dump);

            let index = errors.iter().enumerate().find_map(|(i, error)| {
                if !error.contains("not executed due to a failed transaction") {
                    return Some(i);
                }
                None
            });

            if let Some(index) = index {
                return Err(anyhow::anyhow!(
                    "Import error at index {}:\nError: {}\n SQL: {}\n",
                    index,
                    errors[index],
                    batch[index],
                ));
            } else {
                return Err(anyhow::anyhow!("Import errors:\n{}\n", errors.join("\n")));
            }
        }

        Ok(())
    }

    fn sql(&self, sql: &str) -> Result<()> {
        let res = self.http
            .post(format!("{}/sql", self.endpoint))
            .header("Accept", "application/json")
            .header("Surreal-NS", &self.namespace)
            .header("Surreal-DB", &self.database)
            .basic_auth(&self.username, Some(&self.password))
            .body(sql.to_owned())
            .send()?;

        if !res.status().is_success() {
            return Err(anyhow::anyhow!(
                "Failed to run sql query; error: {}\n{}\nSQL:{}",
                res.status(),
                res.text()?,
                sql,
            ));
        }

        let results = res.json::<Vec<serde_json::Value>>()?;
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
