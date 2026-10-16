//! Temporary Firefox login profiles shared with Moth Controller.
//! Never copies, removes or resets an existing Firefox profile.
use std::{
    env,
    net::{Ipv4Addr, SocketAddrV4, TcpListener},
    path::{Path, PathBuf},
};
use tokio::process::{Child, Command};

pub const GAME_URL: &str = "https://pokeidle.io/app";

#[derive(Clone, Debug)]
pub struct Profile {
    pub slot: usize,
    pub path: PathBuf,
    pub port: u16,
}

impl Profile {
    pub fn new(slot: usize) -> Result<Self, String> {
        if slot >= 4 {
            return Err("Only four game profiles are supported".into());
        }
        Ok(Self {
            slot,
            path: data_root()?.join("Profiles").join(format!("Game{}", slot + 1)),
            port: 27701 + slot as u16,
        })
    }

    pub fn id(&self) -> String {
        format!("Game{}", self.slot + 1)
    }

    pub fn label(&self) -> String {
        format!("Game {}", self.slot + 1)
    }
}

fn data_root() -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        env::var_os("LOCALAPPDATA")
            .map(|root| PathBuf::from(root).join("Moth").join("PokeIdle"))
            .ok_or_else(|| "LOCALAPPDATA is unavailable".into())
    }
    #[cfg(not(windows))]
    {
        env::var_os("XDG_DATA_HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/share")))
            .map(|root| root.join("Moth").join("PokeIdle"))
            .ok_or_else(|| "Cannot locate the user's data directory".into())
    }
}

fn executable() -> Result<PathBuf, String> {
    if let Some(candidate) = env::var_os("MOTH_FIREFOX") {
        let candidate = PathBuf::from(candidate);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    let mut candidates = Vec::new();
    #[cfg(windows)]
    for root in ["PROGRAMFILES", "PROGRAMFILES(X86)", "LOCALAPPDATA"] {
        if let Some(base) = env::var_os(root) {
            let base = PathBuf::from(base);
            for sub in [
                "Firefox Developer Edition/firefox.exe",
                "Mozilla Firefox Developer Edition/firefox.exe",
                "Mozilla Firefox/firefox.exe",
            ] {
                candidates.push(base.join(sub));
            }
        }
    }
    #[cfg(not(windows))]
    {
        candidates.extend([
            "/usr/bin/firefox-developer-edition",
            "/usr/bin/firefox",
            "/usr/local/bin/firefox",
        ].into_iter().map(PathBuf::from));
    }
    if let Some(path) = env::var_os("PATH") {
        for base in env::split_paths(&path) {
            for exe in if cfg!(windows) {
                &["firefox.exe", "firefox-developer-edition.exe"][..]
            } else {
                &["firefox-developer-edition", "firefox"][..]
            } {
                candidates.push(base.join(exe));
            }
        }
    }
    candidates.into_iter().find(|path| path.is_file())
        .ok_or_else(|| "Firefox not found. Install Firefox Developer Edition or set MOTH_FIREFOX.".into())
}

pub fn launch(profile: &Profile) -> Result<Child, String> {
    if !profile.path.is_dir() {
        return Err(format!(
            "Existing profile {} not found. Use Moth Controller to initialize it first.",
            profile.path.display()
        ));
    }
    // The controller/another BiDi owner may already be running. Never attach,
    // steal its single Firefox BiDi session, or kill an unrelated Firefox.
    let port = SocketAddrV4::new(Ipv4Addr::LOCALHOST, profile.port);
    let lease = TcpListener::bind(port)
        .map_err(|_| format!("BiDi port {} is occupied. Close Moth Controller's session for this profile first.", profile.port))?;
    drop(lease);

    let mut cmd = Command::new(executable()?);
    cmd.arg("--no-remote")
        .arg(format!("--remote-debugging-port={}", profile.port))
        .arg("--profile")
        .arg(&profile.path)
        // The preload observer must be installed *before* the game loads.
        .arg("--new-window")
        .arg("about:blank")
        .kill_on_drop(true);
    cmd.spawn().map_err(|e| format!("Unable to launch Firefox: {e}"))
}

pub async fn close_owned(child: &mut Child) {
    match child.try_wait() {
        Ok(Some(_)) => return,
        Err(_) => return,
        Ok(None) => {}
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn profile_names_and_ports_match_moth_controller() {
        for slot in 0..4 {
            let p = Profile::new(slot).unwrap();
            assert_eq!(p.id(), format!("Game{}", slot + 1));
            assert_eq!(p.port, 27701 + slot as u16);
            assert!(p.path.ends_with(Path::new(&format!("Game{}", slot + 1))));
        }
        assert!(Profile::new(4).is_err());
    }
}
