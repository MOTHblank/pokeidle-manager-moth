//! Pure native hunt forecast. No page injection or userscript runtime.
//!
//! Inspired by Hunt Atlas's bounded sustained-rotation model, not the legacy
//! Manager summed-DPS heuristic. Predictions are labeled as predictions,
//! never as measured rates. The source model remains an approximation until
//! calibrated using real combat receipts and repeated HP observations.
use crate::domain::{AccountSnapshot, Pokemon};
use crate::hunt_observations::HpSamples;
use serde::Deserialize;
use std::collections::HashMap;

const POTENCY: [f64; 5] = [0.0, 0.05, 0.10, 0.25, 1.0];
const QUALITY_EXPONENT: [f64; 6] = [0.95, 0.8, 0.8, 0.8, 0.8, 0.95];
const WAVE_MS: f64 = 2600.0;
const MOVE_MS: f64 = 600.0;

#[derive(Debug, Clone, Deserialize)]
pub struct Species {
    pub id: u64,
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "t")]
    pub types: Vec<String>,
    #[serde(rename = "b")]
    pub base: Vec<f64>,
    #[serde(rename = "m", default)]
    pub moves: Vec<(String, String, u8, f64, f64, u32)>,
    #[serde(rename = "l", default)]
    pub loot: Vec<(String, f64, f64, f64, f64)>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct ReferenceHunt {
    #[serde(rename = "s")]
    pub slug: String,
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "a")]
    pub area: String,
    #[serde(rename = "nv")]
    pub level: u32,
    #[serde(rename = "e")]
    pub spawns: Vec<(u64, f64)>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct Reference {
    #[serde(rename = "tipos")]
    pub types: HashMap<String, HashMap<String, f64>>,
    #[serde(rename = "especies")]
    pub species: Vec<Species>,
    #[serde(rename = "hunts")]
    pub hunts: Vec<ReferenceHunt>,
}
impl Reference {
    pub fn parse(value: serde_json::Value) -> Result<Self, String> {
        let result: Self = serde_json::from_value(value)
            .map_err(|_| "Hunt reference is incompatible with the native model".to_string())?;
        if result.species.is_empty() || result.hunts.is_empty() {
            return Err("Hunt reference is empty".into());
        }
        Ok(result)
    }
}

#[derive(Debug, Clone)]
pub struct Forecast {
    pub slug: String,
    pub trainer_xp_h: Option<u64>,
    pub pokemon_xp_h: Option<u64>,
    pub gold_h: Option<u64>,
    pub kills_h: Option<u64>,
    pub hits_per_kill: Option<f64>,
    pub confidence: &'static str,
}
#[derive(Clone)]
struct Attack {
    damage: f64,
    cooldown: f64,
    slot: usize,
}
#[derive(Clone)]
struct Enemy {
    weight: f64,
    hp: f64,
    hp_samples: Vec<f64>,
    basic: f64,
    skills: Vec<Attack>,
    loot: f64,
}
fn battle_stat(species: &Species, index: usize, iv: f64, level: f64, quality: f64, factor: f64) -> f64 {
    ((species.base[index] + 2.0 * iv) * level / 100.0
        * quality.powf(QUALITY_EXPONENT[index]) * factor).round().max(1.0)
}
fn active_stats(lead: &Pokemon, species: &Species) -> Option<[f64; 6]> {
    let ivs = lead.ivs.as_ref()?;
    let values = [
        ivs.hp?, ivs.atk?, ivs.def?, ivs.sp_atk?, ivs.sp_def?, ivs.speed?
    ];
    if species.base.len() < 6 || lead.level == 0 { return None; }
    let quality = lead.quality?;
    let potency = lead.potencia?;
    if !(quality > 0.0) || !(1..=5).contains(&potency) { return None; }
    let factor = (1.0 + POTENCY[(potency - 1) as usize]) *
        if lead.shiny { 3.0 } else { 1.0 };
    let mut stats = [0.0; 6];
    for i in 0..6 {
        stats[i] = battle_stat(species, i, values[i] as f64,
            lead.level as f64, quality, factor);
    }
    Some(stats)
}
fn effectiveness(chart: &HashMap<String, HashMap<String, f64>>,
    move_type: &str, target: &[String]) -> f64 {
    target.iter().map(|t| {
        let raw = chart.get(move_type).and_then(|v| v.get(t)).copied().unwrap_or(1.0);
        if raw > 1.0 { 1.0 + (raw - 1.0) * 1.5 }
        else if raw < 1.0 { raw / 1.5 }
        else { 1.0 }
    }).product()
}
fn damage(level: u32, power: f64, atk: f64, def: f64,
    move_type: &str, lead_types: &[String], target_types: &[String],
    chart: &HashMap<String, HashMap<String, f64>>) -> f64 {
    if level == 0 || power <= 0.0 || atk <= 0.0 || def <= 0.0 { return 0.0; }
    let stab = if lead_types.iter().any(|t| t == move_type) { 1.5 } else { 1.0 };
    let eff = effectiveness(chart, move_type, target_types);
    (((2.0 * level as f64 / 5.0 + 2.0) * power * atk / def) / 50.0 + 2.0)
        * stab * eff * 0.925
}
fn bonus(snapshot: &AccountSnapshot, trainer: bool) -> f64 {
    let state = &snapshot.state;
    let xp = &state.xp_bonus;
    let event = if trainer { xp.event_trainer_pct } else { xp.event_pokemon_pct };
    let f = |v: Option<f64>| -> f64 { v.unwrap_or(0.0).max(0.0) / 100.0 };
    (if state.vip_active.unwrap_or(false) {1.5} else {1.0})
        * (1.0 + f(xp.guild_rank_pct))
        * (if xp.guild_boost_active.unwrap_or(false) {1.1} else {1.0})
        * (1.0 + f(xp.twitch_pct))
        * (1.0 + f(event))
}
fn game_xp(level: u32) -> f64 {
    let level = level as f64;
    if level <= 150.0 { (0.6 * level * level).floor() + 8.0 }
    else { (13_500.0 * (level / 150.0).powf(1.25)).floor() }
}
fn game_gold(level: u32) -> f64 {
    (36.0 * (level.min(25_000) as f64).powf(0.6007)).round()
}
fn cooldown(base_ms: f64, base_speed: f64, iv: u64) -> f64 {
    if !(base_ms > 0.0) { return 10_000.0; }
    let speed_factor = (if base_speed <= 120.0 { base_speed / 20.0 }
        else { 6.0 + (base_speed - 120.0) / 40.0 }).min(7.0);
    (base_ms * (1.0 - speed_factor * iv.min(32) as f64 / 3200.0))
        .round().max(400.0)
}
fn build_enemy(species: &Species, lead: &Pokemon, lead_species: &Species,
    stats: [f64; 6], target_level: u32, weight: f64,
    chart: &HashMap<String, HashMap<String, f64>>,
    observed_hp: &[f64]) -> Option<Enemy> {
    if species.base.len() < 6 || target_level == 0 || weight <= 0.0 { return None; }
    let enemy_def = ((species.base[2] + 32.0) * target_level as f64 / 100.0).max(1.0);
    let enemy_spdef = ((species.base[4] + 32.0) * target_level as f64 / 100.0).max(1.0);
    let hp = (((species.base[0] + 32.0) * target_level as f64 / 100.0) * 60.0)
        .max(24.0);
    let iv = lead.ivs.as_ref()?.speed?;
    // Basic attack uses first own type, switching only when immune.
    let basic_type = lead_species.types.first()?;
    let basic_type = if effectiveness(chart, basic_type, &species.types) == 0.0 {
        lead_species.types.get(1).unwrap_or(basic_type)
    } else { basic_type };
    let basic = damage(lead.level, 30.0, stats[1], enemy_def,
        basic_type, &lead_species.types, &species.types, chart);
    let mut skills = Vec::new();
    for (slot, (_, move_type, special, power, cd_ms, min_level)) in lead_species.moves.iter().enumerate() {
        if *min_level > lead.level { continue; }
        let skill_damage = damage(lead.level, *power,
            if *special != 0 { stats[3] } else { stats[1] },
            if *special != 0 { enemy_spdef } else { enemy_def },
            move_type, &lead_species.types, &species.types, chart);
        skills.push(Attack { slot, damage: skill_damage,
            cooldown: cooldown(*cd_ms, lead_species.base[5], iv) });
    }
    if basic <= 0.0 && skills.iter().all(|s| s.damage <= 0.0) { return None; }
    let loot = species.loot.iter().map(|(_, chance, lo, hi, gold)|
        (chance / 100.0) * (lo + hi) * 0.5 * gold).sum();
    let hp_samples: Vec<f64> = if observed_hp.len() >= 6 {
        observed_hp.iter().copied().filter(|hp| hp.is_finite() && *hp > 0.0)
            .collect()
    } else { Vec::new() };
    Some(Enemy { weight, hp, hp_samples, basic, skills, loot })
}
fn simulate(enemies: &[Enemy], speed_iv: u64, mobs: usize) -> Option<(f64, f64)> {
    if enemies.is_empty() { return None; }
    let count = (mobs.clamp(1, 16)) * 6;
    let warmup = mobs.clamp(1, 16);
    let weight_total: f64 = enemies.iter().map(|enemy| enemy.weight).sum();
    if weight_total <= 0.0 { return None; }
    let global_ms = (900.0 - 10.0 * speed_iv.min(32) as f64).max(400.0);
    let basic_cd = (2000.0 - 10.0 * speed_iv.min(32) as f64).max(400.0);
    let max_slot = enemies.iter().flat_map(|enemy| enemy.skills.iter().map(|s| s.slot))
        .max().unwrap_or(0);
    let mut ready = vec![0.0_f64; max_slot + 1];
    let mut basic_ready = 0.0_f64;
    let mut elapsed = 0.0_f64;
    let mut measured_start = 0.0_f64;
    let mut total_hits = 0_usize;
    let mut target_visits = vec![0_usize; enemies.len()];
    // Match Hunt Atlas: discard the startup burst when every move is
    // initially ready at t=0; interleave spawn weights across six waves.
    let stride = count / 2 + if mobs % 2 == 1 {2} else {1};
    for visit in 0..count {
        if visit == warmup { measured_start = elapsed; }
        let point = (((visit * stride) % count) as f64 + 0.5) * weight_total / count as f64;
        let mut index = 0;
        let mut threshold = enemies[0].weight;
        while point >= threshold && index + 1 < enemies.len() {
            index += 1;
            threshold += enemies[index].weight;
        }
        let enemy = &enemies[index];
        let visits = target_visits[index];
        target_visits[index] += 1;
        let mut hp = if enemy.hp_samples.len() >= 6 {
            enemy.hp_samples[visits % enemy.hp_samples.len()]
        } else { enemy.hp };
        let mut hits = 0_usize;
        for _ in 0..350 {
            if hp <= 0.0 { break; }
            let best = enemy.skills.iter()
                .filter(|attack| ready[attack.slot] <= elapsed && attack.damage > 0.0)
                .max_by(|a,b| a.damage.total_cmp(&b.damage));
            if let Some(skill) = best {
                let rolls = [0.85, 0.90, 0.95, 1.0];
                let roll = rolls[(visit + 3 * hits) % 4] / 0.925;
                hp -= skill.damage * roll;
                ready[skill.slot] = elapsed + skill.cooldown;
                elapsed += global_ms;
                hits += 1;
            } else if enemy.basic > 0.0 && basic_ready <= elapsed {
                let rolls = [0.85, 0.90, 0.95, 1.0];
                hp -= enemy.basic * rolls[(visit + 3 * hits) % 4] / 0.925;
                basic_ready = elapsed + basic_cd;
                elapsed += global_ms;
                hits += 1;
            } else {
                let next = enemy.skills.iter().filter(|s| s.damage > 0.0)
                    .map(|s| ready[s.slot]).fold(f64::INFINITY, f64::min)
                    .min(if enemy.basic > 0.0 { basic_ready } else { f64::INFINITY });
                if !next.is_finite() || next <= elapsed { return None; }
                elapsed = next;
            }
        }
        if hp > 0.0 { return None; }
        if visit >= warmup { total_hits += hits; }
        elapsed += MOVE_MS + WAVE_MS / mobs.max(1) as f64;
    }
    let measured = count - warmup;
    if measured == 0 { return None; }
    let raw = (elapsed - measured_start) / measured as f64;
    let ideal_one_hit = global_ms + MOVE_MS + WAVE_MS / mobs as f64;
    let upstream_typical = 900.0 + MOVE_MS + 2000.0 + WAVE_MS / mobs as f64;
    // Non-measured pacing allowance from upstream hunt heuristic.
    let paced = raw + (upstream_typical - ideal_one_hit).max(0.0);
    (paced.is_finite() && paced > 0.0).then_some((paced, total_hits as f64 / measured as f64))
}
pub fn forecast(snapshot: &AccountSnapshot, reference: &Reference,
                observed: &HpSamples) -> Vec<Forecast> {
    let Some(lead) = snapshot.state.active_id
        .and_then(|id| snapshot.state.pokemon.iter().find(|p| p.id == id))
    else { return Vec::new(); };
    let species: HashMap<u64, &Species> = reference.species.iter().map(|s| (s.id, s)).collect();
    let Some(lead_species) = lead.species_id.and_then(|id| species.get(&id).copied())
    else { return Vec::new(); };
    let Some(stats) = active_stats(lead, lead_species) else { return Vec::new(); };
    let Some(iv) = lead.ivs.as_ref().and_then(|i| i.speed) else { return Vec::new(); };
    let reference_hunts: HashMap<_,_> = reference.hunts.iter()
        .map(|h| (h.slug.as_str(), h)).collect();
    snapshot.state.hunts.iter().filter_map(|hunt| {
        let ref_hunt = reference_hunts.get(hunt.slug.as_str())?;
        let level = hunt.level.unwrap_or(ref_hunt.level);
        if level > snapshot.state.level.unwrap_or_default() { return None; }
        let spawns: Vec<(u64, f64)> = if hunt.species.iter().any(|row| row.species_id.is_some()) {
            hunt.species.iter().filter_map(|s| Some((s.species_id?, s.weight?))).collect()
        } else { ref_hunt.spawns.clone() };
        let enemies: Vec<Enemy> = spawns.iter().filter_map(|(id, weight)| {
            let hp_sample = observed.get(&(
                snapshot.account.id.clone(), hunt.slug.clone(), *id, level
            )).map(Vec::as_slice).unwrap_or(&[]);
            build_enemy(species.get(id)?, lead, lead_species, stats,
                level, *weight, &reference.types, hp_sample)
        }).collect();
        let coverage = enemies.len() == spawns.len() && !spawns.is_empty();
        let hp_observed = enemies.iter().filter(|e| e.hp_samples.len() >= 6).count();
        let calculated = if coverage {
            simulate(&enemies, iv, hunt.total_spawns.unwrap_or(6) as usize)
        } else { None };
        let total_weight: f64 = enemies.iter().map(|e| e.weight).sum();
        let avg_loot = if total_weight > 0.0 {
            enemies.iter().map(|e| e.loot * e.weight).sum::<f64>() / total_weight
        } else { 0.0 };
        let (trainer_xp_h, pokemon_xp_h, gold_h, kills_h, hits_per_kill) =
            if let Some((ms, hits)) = calculated {
                let kills = 3_600_000.0 / ms;
                let xp = game_xp(level) * kills;
                let gold = (game_gold(level) + avg_loot) * kills;
                let to_u64 = |x: f64| x.is_finite().then_some(x.max(0.0).round() as u64);
                (to_u64(xp * bonus(snapshot, true)),
                 to_u64(xp * bonus(snapshot, false)),
                 to_u64(gold), to_u64(kills), Some(hits))
            } else { (None,None,None,None,None) };
        Some(Forecast {
            slug: hunt.slug.clone(),
            trainer_xp_h, pokemon_xp_h, gold_h, kills_h, hits_per_kill,
            confidence: if !coverage || calculated.is_none() {
                "Sem modelo"
            } else if hp_observed == enemies.len() {
                "Modelo · HP observado"
            } else if hp_observed > 0 {
                "Modelo · HP parcial"
            } else {
                "Modelo · HP estimado"
            },
        })
    }).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn immunity_does_not_create_fake_damage() {
        let chart = HashMap::from([("NORMAL".into(), HashMap::from([("GHOST".into(), 0.0)]))]);
        assert_eq!(damage(100, 30.0, 250.0, 150.0, "NORMAL",
            &["NORMAL".into()], &["GHOST".into()], &chart), 0.0);
    }
    #[test]
    fn sustained_rotation_respects_cooldowns() {
        let s = vec![Enemy { weight: 1.0, hp: 120.0, hp_samples: vec![], basic: 20.0,
            skills: vec![Attack {slot: 0, damage: 80.0, cooldown: 10_000.0}], loot: 0.0 }];
        let (ms, hits) = simulate(&s, 16, 1).unwrap();
        assert!(ms > 2000.0);
        assert!(hits > 1.0);
    }
    #[test]
    fn observed_hp_changes_only_that_enemy_sustained_combat() {
        let plain = Enemy {
            weight: 1.0, hp: 100.0, hp_samples: vec![], basic: 50.0,
            skills: vec![], loot: 0.0
        };
        let mut observed = plain.clone();
        observed.hp_samples = vec![1_000.0; 6];
        let (base, _) = simulate(&[plain], 16, 1).unwrap();
        let (measured_hp, _) = simulate(&[observed], 16, 1).unwrap();
        assert!(measured_hp > base);
    }
    #[test]
    fn trainer_and_pokemon_modifiers_are_separate() {
        let s = AccountSnapshot {
            account: crate::accounts::new_record("id".into(), "nick".into(), "#fff".into()),
            state: crate::domain::AccountState::default(),
            metrics: crate::domain::AccountMetrics::default(),
        };
        assert_eq!(bonus(&s, true), bonus(&s, false));
    }
}
