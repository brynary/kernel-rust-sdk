//! Check that direct-to-VM routing works against the live API: run the same
//! calls with routing on and off and compare latency.
//!
//! Run: cargo run --example routing_check

use std::time::Instant;

use kernel::types::browsers::ProcessExecParams;
use kernel::types::BrowserCreateParams;
use kernel::{Kernel, RoutingConfig};

async fn time_execs(client: &Kernel, id: &str) -> kernel::Result<f64> {
    let start = Instant::now();
    for _ in 0..5 {
        client
            .browsers()
            .process()
            .exec(id, ProcessExecParams::new("true"))
            .await?;
    }
    Ok(start.elapsed().as_secs_f64() / 5.0 * 1000.0)
}

#[tokio::main]
async fn main() -> kernel::Result<()> {
    let routed = Kernel::builder().build()?;
    let direct_off = Kernel::builder()
        .browser_routing(RoutingConfig::disabled())
        .build()?;
    let browser = routed
        .browsers()
        .create(BrowserCreateParams {
            headless: Some(true),
            timeout_seconds: Some(60),
            ..Default::default()
        })
        .await?;
    let id = browser.session_id.clone();
    let result = async {
        // Warm up both connection pools.
        time_execs(&routed, &id).await?;
        time_execs(&direct_off, &id).await?;
        let on = time_execs(&routed, &id).await?;
        let off = time_execs(&direct_off, &id).await?;
        println!("routing on:  {on:.0} ms per exec");
        println!("routing off: {off:.0} ms per exec");
        println!(
            "route still cached: {}",
            routed.browser_route_cache().get(&id).is_some()
        );
        kernel::Result::Ok(())
    }
    .await;
    routed.browsers().delete_by_id(&id).await?;
    result
}
