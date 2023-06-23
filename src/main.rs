use std::{
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use datafusion::{
    arrow::{record_batch::RecordBatch, util::pretty::pretty_format_batches},
    error::DataFusionError,
    prelude::{ParquetReadOptions, SessionContext},
};
use tokio::task::JoinSet;

const QUERIES: &[&'static str] = &[
    "explain analyze select * from test",
    "explain analyze select * from test where response_status >= 400",
    "explain analyze select * from test where request_bytes >= 0 and request_method = 'POST'",
];

#[tokio::main]
async fn main() -> Result<(), DataFusionError> {
    let count: usize = env::args()
        .nth(1)
        .and_then(|x| x.parse::<usize>().ok())
        .expect("Count should be a valid number passed as first argument of this program");

    let query = env::args().nth(2).unwrap_or(QUERIES[0].to_string());

    let ctx = SessionContext::new();
    // create the dataframe
    ctx.register_parquet("test", "logs.parquet", ParquetReadOptions::default())
        .await?;

    let ctx = Arc::new(ctx);

    let mut query_tasks = JoinSet::new();

    for _ in 0..count {
        query_tasks.spawn(run_query(query.clone(), Arc::clone(&ctx)));
    }

    while let Some(res) = query_tasks.join_next().await {
        let (duration, res) = res.expect("joined")?;
        println!();
        println!("Time taken: {}", duration.as_secs());
        println!("{}", pretty_format_batches(&res)?);
    }

    Ok(())
}

async fn run_query(
    query: String,
    ctx: Arc<SessionContext>,
) -> Result<(Duration, Vec<RecordBatch>), DataFusionError> {
    let time = Instant::now();

    let df = ctx.sql(&query).await?;
    // execute the plan
    let rb = df.collect().await?;

    Ok((time.elapsed(), rb))
}
