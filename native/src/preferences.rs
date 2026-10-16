//! Settings for the native shell only. Never alters the Firefox profiles or
//! the authoritative game session engine.
use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct NativePreferences {
    pub interface_refresh_seconds: u32,
    pub restore_enabled_profiles: bool,
}
impl Default for NativePreferences {
    fn default() -> Self {
        Self { interface_refresh_seconds: 1, restore_enabled_profiles: true }
    }
}
impl NativePreferences {
    pub fn normalized(mut self) -> Self {
        if ![1, 2, 5].contains(&self.interface_refresh_seconds) {
            self.interface_refresh_seconds = 1;
        }
        self
    }
    pub fn load(path: &Path) -> Self {
        fs::read_to_string(path).ok()
            .and_then(|content| serde_json::from_str::<Self>(&content).ok())
            .unwrap_or_default()
            .normalized()
    }
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let normalized = self.clone().normalized();
        let parent = path.parent().ok_or("Missing settings directory")?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        let serialized = serde_json::to_vec_pretty(&normalized)
            .map_err(|error| error.to_string())?;
        let staged = path.with_extension("json.pending");
        fs::write(&staged, &serialized).map_err(|error| error.to_string())?;
        if fs::rename(&staged, path).is_err() {
            // On Windows std::fs::rename won't replace an existing destination.
            // Prefer a written fallback to accidentally losing user preferences.
            fs::write(path, &serialized).map_err(|error| error.to_string())?;
            let _ = fs::remove_file(&staged);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_refresh_interval_is_clamped() {
        let input = serde_json::json!({
            "interface_refresh_seconds": 500,
            "restore_enabled_profiles": false
        });
        let parsed: NativePreferences = serde_json::from_value(input).unwrap();
        let result = parsed.normalized();
        assert_eq!(result.interface_refresh_seconds, 1);
        assert!(!result.restore_enabled_profiles);
    }
    #[test]
    fn old_documents_have_safe_defaults() {
        let parsed: NativePreferences = serde_json::from_str("{}").unwrap();
        assert_eq!(parsed.interface_refresh_seconds, 1);
        assert!(parsed.restore_enabled_profiles);
    }
}