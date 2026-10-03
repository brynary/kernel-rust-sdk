//! `client.config_registry().analyses().wait_for_result(...)`: poll an
//! analysis until it finishes. Ported from the first-party SDKs.

use std::time::{Duration, Instant};

use crate::resources::config_registry::analyses::Analyses;
use crate::types::{ConfigRegistryAnalysisStatus, ConfigRegistryResponse};
use crate::{Error, RequestOptions, Result};

/// The default time between polls.
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Options for [`Analyses::wait_for_result`].
#[derive(Debug, Clone)]
pub struct WaitOptions {
    /// The time between polls. A random jitter of ±10% is added.
    pub poll_interval: Duration,
    /// A soft deadline. An in-flight request and its retries may finish
    /// after it. Timing out does not cancel the remote analysis.
    pub max_wait: Option<Duration>,
}

impl Default for WaitOptions {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_wait: None,
        }
    }
}

impl Analyses<'_> {
    /// Wait for an analysis to finish and return its complete result.
    ///
    /// The first retrieval happens immediately. If `max_wait` passes first,
    /// this returns [`Error::Other`] with the poll count and last status.
    pub async fn wait_for_result(
        &self,
        id: &str,
        options: WaitOptions,
    ) -> Result<ConfigRegistryResponse> {
        if options.poll_interval.is_zero() {
            return Err(Error::Config("poll_interval must be positive".to_owned()));
        }
        let started = Instant::now();
        let deadline = options.max_wait.map(|d| started + d);
        let client = self
            .client
            .with_options(RequestOptions::new().header("X-Stainless-Poll-Helper", "true"));
        let analyses = Analyses { client: &client };
        let mut polls = 0u32;
        let mut last_status: Option<String> = None;

        loop {
            if polls > 0 && deadline.is_some_and(|d| Instant::now() >= d) {
                return Err(timeout_error(id, polls, &last_status, started));
            }
            let response = analyses.retrieve(id).await?;
            polls += 1;
            let (finished, status) = analysis_finished(&response, id)?;
            last_status = Some(status);
            if finished {
                return Ok(response);
            }

            let mut delay = jitter(options.poll_interval);
            if let Some(d) = deadline {
                let remaining = d.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(timeout_error(id, polls, &last_status, started));
                }
                delay = delay.min(remaining);
            }
            tokio::time::sleep(delay).await;
        }
    }
}

fn analysis_finished(response: &ConfigRegistryResponse, id: &str) -> Result<(bool, String)> {
    let analysis = response.analysis.as_ref().ok_or_else(|| {
        Error::Other(format!(
            "config registry response for {id:?} is missing an analysis"
        ))
    })?;
    if analysis.id.is_empty() {
        return Err(Error::Other(format!(
            "config registry response for {id:?} has no valid analysis ID"
        )));
    }
    if analysis.id != id {
        return Err(Error::Other(format!(
            "config registry response for {id:?} returned analysis {:?}",
            analysis.id
        )));
    }
    let status = analysis.status.as_str().to_owned();
    if status.is_empty() {
        return Err(Error::Other(format!(
            "config registry analysis {id:?} has no valid status"
        )));
    }
    let terminal = matches!(
        analysis.status,
        ConfigRegistryAnalysisStatus::Completed
            | ConfigRegistryAnalysisStatus::Failed
            | ConfigRegistryAnalysisStatus::Canceled
            | ConfigRegistryAnalysisStatus::Expired
    );
    Ok((analysis.finished_at.is_some() || terminal, status))
}

fn timeout_error(id: &str, polls: u32, last_status: &Option<String>, started: Instant) -> Error {
    Error::Other(format!(
        "timed out waiting for config registry analysis {id:?} after {:.1}s and {polls} polls; \
         last status was {last_status:?}",
        started.elapsed().as_secs_f64()
    ))
}

fn jitter(d: Duration) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|t| t.subsec_nanos())
        .unwrap_or(0);
    let factor = 0.9 + 0.2 * (f64::from(nanos % 1_000_000) / 1_000_000.0);
    d.mul_f64(factor)
}
