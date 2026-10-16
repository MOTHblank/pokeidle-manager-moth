//! Hunt-local, server-observed wild HP. Does not infer damage or XP from
//! these observations and never applies a correction to another hunt.
//! Data remain in process memory for the current Manager run.
use crate::domain::AccountLiveSnapshot;
use std::collections::HashMap;

pub type HpSamples = HashMap<(String, String, u64, u32), Vec<f64>>;

#[derive(Default)]
pub struct HpCollector {
    last_encounter: HashMap<String, (u64, u64, u64, u64)>,
    samples: HpSamples,
}

impl HpCollector {
    pub fn absorb(&mut self, lives: &[AccountLiveSnapshot]) {
        for entry in lives {
            let state = &entry.state;
            let (Some(session), Some(hunt), Some(wild)) = (
                state.hunt_session.as_ref(), state.hunt_slug.as_deref(),
                state.wild.as_ref()
            ) else { continue; };
            if hunt != session.hunt_slug.as_str() { continue; }
            let Some(species) = wild.species_id else { continue; };
            if wild.max_hp == 0 || wild.level == 0 { continue; }
            self.record(
                &entry.account.id, hunt, species, wild.level,
                session.started_at_ms, session.kills,
                wild.slot, wild.max_hp
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &mut self,
        account: &str, hunt: &str, species: u64, level: u32,
        session: u64, kills: u64, slot: u64, hp: u64
    ) {
        let encounter = (session, kills, slot, species);
        if self.last_encounter.get(account).copied() == Some(encounter) {
            return;
        }
        self.last_encounter.insert(account.to_owned(), encounter);
        let key = (account.to_owned(), hunt.to_owned(), species, level);
        let sample = self.samples.entry(key).or_default();
        sample.push(hp as f64);
        // Limit history without synthesizing observations or disk persistence.
        if sample.len() > 48 { sample.remove(0); }
        if self.samples.len() > 512 { self.samples.clear(); }
    }

    pub fn snapshot(&self) -> HpSamples {
        self.samples.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn keeps_species_and_hunts_separate_and_deduplicates_polling() {
        let mut store = HpCollector::default();
        store.record("Game1", "forest", 123, 100, 5, 7, 0, 2400);
        store.record("Game1", "forest", 123, 100, 5, 7, 0, 2400);
        store.record("Game1", "forest", 123, 100, 5, 8, 0, 2440);
        store.record("Game1", "forest", 124, 100, 5, 8, 0, 2500);
        store.record("Game1", "field", 123, 100, 9, 0, 0, 3000);
        assert_eq!(store.samples[&("Game1".into(), "forest".into(), 123, 100)],
            [2400., 2440.]);
        assert_eq!(store.samples[&("Game1".into(), "forest".into(), 124, 100)],
            [2500.]);
        assert_eq!(store.samples[&("Game1".into(), "field".into(), 123, 100)],
            [3000.]);
        assert_eq!(store.samples.len(), 3);
    }
}