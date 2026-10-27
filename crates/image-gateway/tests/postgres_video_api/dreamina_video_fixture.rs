//! Debug-only loopback transport for the real video stream, stager and PG reducer.
//! This does not exercise public DNS/TLS or relax the release download policy.
use super::*;
use std::{
    ffi::OsString,
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::{sync::oneshot, task::JoinHandle};

const ENV: &str = "GATEWAY_TEST_GENERATED_VIDEO_URL";

pub(super) struct VideoTransportFixture {
    previous: Option<OsString>,
    requests: Arc<AtomicUsize>,
    received_auth: Arc<AtomicBool>,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<Result<(), std::io::Error>>>,
}

impl VideoTransportFixture {
    pub(super) async fn start(_serial: &tokio::sync::MutexGuard<'_, ()>) -> TestResult<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(debug_error)?;
        let address = listener.local_addr().map_err(debug_error)?;
        let requests = Arc::new(AtomicUsize::new(0));
        let received_auth = Arc::new(AtomicBool::new(false));
        let app = axum::Router::new().route(
            "/fixture.mp4",
            axum::routing::get({
                let requests = requests.clone();
                let received_auth = received_auth.clone();
                move |headers: axum::http::HeaderMap| {
                    requests.fetch_add(1, Ordering::SeqCst);
                    received_auth.fetch_or(
                        headers.contains_key(header::AUTHORIZATION),
                        Ordering::SeqCst,
                    );
                    async { ([(header::CONTENT_TYPE, "video/mp4")], VIDEO_BYTES) }
                }
            }),
        );
        let (shutdown, stop) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = stop.await;
                })
                .await
        });
        let previous = env::var_os(ENV);
        // All consumers of this dedicated test variable hold the PG video suite's
        // shared serial lock. Drop restores the exact prior process environment.
        unsafe {
            env::set_var(ENV, format!("http://{address}/fixture.mp4"));
        }
        eprintln!(
            "loopback_video_fixture_address={address} mp4_sha256={}",
            hex::encode(Sha256::digest(VIDEO_BYTES))
        );
        Ok(Self {
            previous,
            requests,
            received_auth,
            shutdown: Some(shutdown),
            server: Some(server),
        })
    }

    pub(super) async fn finish(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(mut server) = self.server.take() {
            match tokio::time::timeout(Duration::from_secs(2), &mut server).await {
                Ok(result) => result.map_err(debug_error)?.map_err(debug_error)?,
                Err(_) => {
                    server.abort();
                    return Err("loopback video server did not stop".into());
                }
            }
        }
        let valid =
            self.requests.load(Ordering::SeqCst) == 1 && !self.received_auth.load(Ordering::SeqCst);
        eprintln!(
            "loopback_video_fixture_stopped=true requests={} received_auth={}",
            self.requests.load(Ordering::SeqCst),
            self.received_auth.load(Ordering::SeqCst)
        );
        require(
            valid,
            "video transport must fetch exactly once without upstream authorization",
        )
    }
}

impl Drop for VideoTransportFixture {
    fn drop(&mut self) {
        let previous = self.previous.take();
        unsafe {
            match &previous {
                Some(previous) => env::set_var(ENV, previous),
                None => env::remove_var(ENV),
            }
        }
        eprintln!(
            "loopback_video_fixture_environment_restored={}",
            env::var_os(ENV) == previous
        );
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

pub(super) fn private_submit_runner(root: &Path) -> TestResult<PathBuf> {
    let bytes = fs::read(env!("CARGO_BIN_EXE_remote-submit-runner")).map_err(debug_error)?;
    let runner = root.join("remote-submit-runner");
    fs::write(&runner, &bytes).map_err(debug_error)?;
    fs::set_permissions(&runner, fs::Permissions::from_mode(0o500)).map_err(debug_error)?;
    eprintln!(
        "private_submit_runner_sha256={}",
        hex::encode(Sha256::digest(&bytes))
    );
    Ok(runner)
}
