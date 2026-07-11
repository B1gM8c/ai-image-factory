use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::oneshot;

use super::{LeaseLost, run_until_lease_lost};

struct DropSignal(Arc<AtomicBool>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn lease_loss_cancels_an_in_flight_provider_future() {
    let dropped = Arc::new(AtomicBool::new(false));
    let drop_signal = DropSignal(dropped.clone());
    let (lost_tx, mut lost_rx) = oneshot::channel();
    let provider = async move {
        let _drop_signal = drop_signal;
        std::future::pending::<()>().await;
    };

    lost_tx.send(()).expect("lease-loss receiver must exist");
    let result = run_until_lease_lost(&mut lost_rx, provider).await;

    assert_eq!(result, Err(LeaseLost));
    assert!(
        dropped.load(Ordering::SeqCst),
        "provider future continued after lease loss"
    );
}
