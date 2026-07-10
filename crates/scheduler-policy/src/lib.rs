use std::collections::HashMap;

const FIXED_POINT_SCALE: u128 = 1_000_000;
pub const DEFAULT_AGING_QUANTUM_MS: u64 = 30_000;
pub const DEFAULT_AGING_CREDIT: u64 = 250_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Priority(u8);

impl Priority {
    pub const LOW: Self = Self(0);
    pub const NORMAL: Self = Self(1);
    pub const HIGH: Self = Self(2);
    pub const URGENT: Self = Self(3);

    pub const fn new(value: u8) -> Option<Self> {
        if value <= Self::URGENT.0 {
            Some(Self(value))
        } else {
            None
        }
    }

    pub const fn value(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchedulerConfig {
    pub aging_quantum_ms: u64,
    pub aging_credit: u128,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            aging_quantum_ms: DEFAULT_AGING_QUANTUM_MS,
            aging_credit: u128::from(DEFAULT_AGING_CREDIT),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueueItem {
    pub id: String,
    pub scope: String,
    pub priority: Priority,
    pub cost: u64,
    pub enqueued_at_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledItem {
    pub item: QueueItem,
    pub finish_tag: u128,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeWeight(u32);

impl ScopeWeight {
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 { None } else { Some(Self(value)) }
    }

    pub const fn value(self) -> u32 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueError {
    EmptyId,
    EmptyScope,
    ZeroCost,
    DuplicateId,
    InvalidWeight,
}

#[derive(Clone, Debug)]
pub struct FairScheduler {
    config: SchedulerConfig,
    virtual_time: u128,
    next_sequence: u64,
    scope_next_finish: HashMap<String, u128>,
    scope_weights: HashMap<String, ScopeWeight>,
    ready: Vec<ScheduledItem>,
}

impl FairScheduler {
    pub fn new(config: SchedulerConfig) -> Result<Self, EnqueueError> {
        if config.aging_quantum_ms == 0 {
            return Err(EnqueueError::InvalidWeight);
        }
        Ok(Self {
            config,
            virtual_time: 0,
            next_sequence: 0,
            scope_next_finish: HashMap::new(),
            scope_weights: HashMap::new(),
            ready: Vec::new(),
        })
    }

    pub fn set_scope_weight(
        &mut self,
        scope: impl Into<String>,
        weight: ScopeWeight,
    ) -> Result<(), EnqueueError> {
        let scope = scope.into();
        if scope.is_empty() {
            return Err(EnqueueError::EmptyScope);
        }
        self.scope_weights.insert(scope, weight);
        Ok(())
    }

    pub fn enqueue(&mut self, item: QueueItem) -> Result<u64, EnqueueError> {
        if item.id.is_empty() {
            return Err(EnqueueError::EmptyId);
        }
        if item.scope.is_empty() {
            return Err(EnqueueError::EmptyScope);
        }
        if item.cost == 0 {
            return Err(EnqueueError::ZeroCost);
        }
        if self
            .ready
            .iter()
            .any(|scheduled| scheduled.item.id == item.id)
        {
            return Err(EnqueueError::DuplicateId);
        }

        let weight = self
            .scope_weights
            .get(&item.scope)
            .copied()
            .unwrap_or(ScopeWeight(1));
        let previous_finish = self
            .scope_next_finish
            .get(&item.scope)
            .copied()
            .unwrap_or(self.virtual_time);
        let start = previous_finish.max(self.virtual_time);
        let service =
            (u128::from(item.cost) * FIXED_POINT_SCALE).div_ceil(u128::from(weight.value()));
        let finish_tag = start.saturating_add(service);
        self.scope_next_finish
            .insert(item.scope.clone(), finish_tag);

        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.ready.push(ScheduledItem {
            item,
            finish_tag,
            sequence,
        });
        Ok(sequence)
    }

    pub fn cancel(&mut self, id: &str) -> bool {
        let original_len = self.ready.len();
        self.ready.retain(|scheduled| scheduled.item.id != id);
        original_len != self.ready.len()
    }

    pub fn pop(&mut self, now_ms: u64) -> Option<ScheduledItem> {
        let index = self
            .ready
            .iter()
            .enumerate()
            .min_by_key(|(_, item)| self.order_key(item, now_ms))
            .map(|(index, _)| index)?;
        let item = self.ready.swap_remove(index);
        self.virtual_time = self.virtual_time.max(item.finish_tag);
        Some(item)
    }

    pub fn len(&self) -> usize {
        self.ready.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ready.is_empty()
    }

    fn order_key(&self, item: &ScheduledItem, now_ms: u64) -> (i128, u8, u64, u64) {
        let age_ms = now_ms.saturating_sub(item.item.enqueued_at_ms);
        let age_steps = u128::from(age_ms / self.config.aging_quantum_ms);
        let finish_tag = item.finish_tag.min(i128::MAX as u128) as i128;
        let aging_credit = self.config.aging_credit.min(i128::MAX as u128) as i128;
        let age_steps = age_steps.min(i128::MAX as u128) as i128;
        let effective_finish = finish_tag.saturating_sub(aging_credit.saturating_mul(age_steps));
        (
            effective_finish,
            u8::MAX - item.item.priority.value(),
            item.item.enqueued_at_ms,
            item.sequence,
        )
    }
}

impl Default for FairScheduler {
    fn default() -> Self {
        Self::new(SchedulerConfig::default()).expect("default scheduler config is valid")
    }
}

pub fn next_finish_tag(previous_finish: u64, cost: u64, weight: ScopeWeight) -> u64 {
    let service = (u128::from(cost) * FIXED_POINT_SCALE).div_ceil(u128::from(weight.value()));
    u128::from(previous_finish)
        .saturating_add(service)
        .min(u128::from(u64::MAX)) as u64
}

pub fn effective_finish_tag(
    finish_tag: u64,
    enqueued_at_ms: u64,
    priority: Priority,
    now_ms: u64,
    config: SchedulerConfig,
) -> i128 {
    if config.aging_quantum_ms == 0 {
        return i128::from(finish_tag).saturating_sub(i128::from(priority.value()));
    }
    let age_steps = u128::from(now_ms.saturating_sub(enqueued_at_ms) / config.aging_quantum_ms);
    let finish_tag = i128::from(finish_tag);
    let aging_credit = i128::try_from(config.aging_credit).unwrap_or(i128::MAX);
    let age_steps = i128::try_from(age_steps).unwrap_or(i128::MAX);
    let aged = finish_tag.saturating_sub(aging_credit.saturating_mul(age_steps));
    aged.saturating_sub(i128::from(priority.value()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, scope: &str, cost: u64, priority: Priority, at: u64) -> QueueItem {
        QueueItem {
            id: id.to_string(),
            scope: scope.to_string(),
            priority,
            cost,
            enqueued_at_ms: at,
        }
    }

    #[test]
    fn weighted_finish_tags_give_heavier_scopes_more_service() {
        let mut scheduler = FairScheduler::default();
        scheduler
            .set_scope_weight("heavy", ScopeWeight::new(2).unwrap())
            .unwrap();
        for index in 0..6 {
            scheduler
                .enqueue(item(
                    &format!("light-{index}"),
                    "light",
                    1,
                    Priority::NORMAL,
                    0,
                ))
                .unwrap();
            scheduler
                .enqueue(item(
                    &format!("heavy-{index}"),
                    "heavy",
                    1,
                    Priority::NORMAL,
                    0,
                ))
                .unwrap();
        }

        let mut heavy = 0;
        let mut light = 0;
        while let Some(next) = scheduler.pop(0) {
            if next.item.scope == "heavy" {
                heavy += 1;
            } else {
                light += 1;
            }
        }
        assert_eq!((heavy, light), (6, 6));

        let mut scheduler = FairScheduler::default();
        scheduler
            .set_scope_weight("heavy", ScopeWeight::new(2).unwrap())
            .unwrap();
        for index in 0..30 {
            scheduler
                .enqueue(item(
                    &format!("light-{index}"),
                    "light",
                    1,
                    Priority::NORMAL,
                    0,
                ))
                .unwrap();
            scheduler
                .enqueue(item(
                    &format!("heavy-{index}"),
                    "heavy",
                    1,
                    Priority::NORMAL,
                    0,
                ))
                .unwrap();
        }
        let first_ten: Vec<_> = (0..10)
            .filter_map(|_| scheduler.pop(0).map(|item| item.item.scope))
            .collect();
        assert!(first_ten.iter().filter(|scope| *scope == "heavy").count() >= 6);
    }

    #[test]
    fn priority_is_bounded_and_aging_prevents_starvation() {
        assert!(Priority::new(4).is_none());
        assert!(ScopeWeight::new(0).is_none());

        let mut scheduler = FairScheduler::new(SchedulerConfig {
            aging_quantum_ms: 10,
            aging_credit: 10 * FIXED_POINT_SCALE,
        })
        .unwrap();
        scheduler
            .enqueue(item("old-low", "tenant-a", 100, Priority::LOW, 0))
            .unwrap();
        scheduler
            .enqueue(item("new-urgent", "tenant-b", 1, Priority::URGENT, 100))
            .unwrap();

        let first = scheduler.pop(1_000).unwrap();
        assert_eq!(first.item.id, "old-low");
    }

    #[test]
    fn cancellation_and_ties_are_deterministic() {
        let mut scheduler = FairScheduler::default();
        scheduler
            .enqueue(item("first", "tenant", 1, Priority::NORMAL, 0))
            .unwrap();
        scheduler
            .enqueue(item("second", "tenant", 1, Priority::NORMAL, 0))
            .unwrap();
        assert!(scheduler.cancel("first"));
        assert!(!scheduler.cancel("missing"));
        assert_eq!(scheduler.pop(0).unwrap().item.id, "second");
        assert!(scheduler.is_empty());
    }

    #[test]
    fn invalid_items_are_rejected_without_mutating_queue() {
        let mut scheduler = FairScheduler::default();
        for invalid in [
            item("", "tenant", 1, Priority::NORMAL, 0),
            item("job", "", 1, Priority::NORMAL, 0),
            item("job", "tenant", 0, Priority::NORMAL, 0),
        ] {
            assert!(scheduler.enqueue(invalid).is_err());
        }
        scheduler
            .enqueue(item("job", "tenant", 1, Priority::NORMAL, 0))
            .unwrap();
        assert_eq!(
            scheduler.enqueue(item("job", "tenant", 1, Priority::NORMAL, 0)),
            Err(EnqueueError::DuplicateId)
        );
        assert_eq!(scheduler.len(), 1);
    }
}
