//! Live smoke test against the Kernel API. Needs KERNEL_API_KEY.
//!
//! Run: cargo run --example smoke

use futures_util::StreamExt;
use kernel::types::browsers::ProcessExecParams;
use kernel::types::{BrowserCreateParams, BrowserListParams};
use kernel::Kernel;

#[tokio::main]
async fn main() -> kernel::Result<()> {
    let client = Kernel::new()?;

    let browser = client
        .browsers()
        .create(BrowserCreateParams {
            headless: Some(true),
            timeout_seconds: Some(60),
            ..Default::default()
        })
        .await?;
    println!("created browser {}", browser.session_id);

    let result = async {
        let fetched = client
            .browsers()
            .retrieve(&browser.session_id, Default::default())
            .await?;
        println!("retrieved browser, headless={}", fetched.headless);

        let exec = client
            .browsers()
            .process()
            .exec(&browser.session_id, ProcessExecParams::new("echo"))
            .await?;
        println!("exec exit code: {:?}", exec.exit_code);

        let png = client
            .browsers()
            .computer()
            .capture_screenshot(&browser.session_id, Default::default())
            .await?
            .bytes()
            .await?;
        println!("screenshot: {} bytes", png.len());

        let mut pages = client
            .browsers()
            .list(BrowserListParams {
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .into_stream();
        let mut seen = 0;
        while let Some(b) = pages.next().await {
            b?;
            seen += 1;
            if seen >= 3 {
                break;
            }
        }
        println!("listed {seen} browsers across pages");
        kernel::Result::Ok(())
    }
    .await;

    client.browsers().delete_by_id(&browser.session_id).await?;
    println!("deleted browser");
    result
}
