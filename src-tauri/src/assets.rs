use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OnceCell, Semaphore};

const GAME_ORIGIN: &str = "https://pokeidle.io";
const HUNT_GUIDE_ORIGIN: &str = "https://guiapokeidlehardtocapture.site";
const CACHE_VERSION: &str = "catalog-v1.json";
const NEGATIVE_CACHE_FOR: Duration = Duration::from_secs(5 * 60);
const MAX_ASSET_BYTES: usize = 5 * 1024 * 1024;
const MAX_HUNT_REFERENCE_BYTES: usize = 4 * 1024 * 1024;

fn parse_hunt_reference_script(script: &str) -> Result<serde_json::Value, String> {
    let (_, assignment) = script
        .split_once("window.PI_DATA =")
        .ok_or_else(|| "Formato inesperado na referência de hunts.".to_owned())?;
    let assignment = assignment.trim();
    let json = assignment.strip_suffix(';').unwrap_or(assignment).trim();
    let parsed: serde_json::Value = serde_json::from_str(json)
        .map_err(|_| "Não foi possível interpretar a referência de hunts.".to_owned())?;
    if !parsed
        .get("especies")
        .is_some_and(serde_json::Value::is_array)
        || !parsed.get("hunts").is_some_and(serde_json::Value::is_array)
    {
        return Err("A referência de hunts está incompleta.".to_owned());
    }
    Ok(parsed)
}

fn merge_species_looktypes(looktypes: &mut BTreeMap<u64, u64>, document: &serde_json::Value) {
    let Some(creatures) = document
        .get("creatures")
        .and_then(serde_json::Value::as_array)
    else {
        return;
    };
    for creature in creatures {
        let (Some(species_id), Some(looktype)) = (
            creature.get("pokeId").and_then(serde_json::Value::as_u64),
            creature.get("looktype").and_then(serde_json::Value::as_u64),
        ) else {
            continue;
        };
        if species_id > 0 && looktype > 1 {
            looktypes.insert(species_id, looktype);
        }
    }
}

fn merge_sprite_looktype_patches(looktypes: &mut BTreeMap<u64, u64>, document: &serde_json::Value) {
    let Some(patches) = document
        .get("patches")
        .and_then(serde_json::Value::as_array)
    else {
        return;
    };
    for patch in patches {
        let (Some(species_id), Some(looktype)) = (
            patch.get("pokeId").and_then(serde_json::Value::as_u64),
            patch.get("looktype").and_then(serde_json::Value::as_u64),
        ) else {
            continue;
        };
        if species_id > 0 && looktype > 1 && looktype < 70_000 {
            looktypes.insert(species_id, looktype);
        }
    }
}

#[derive(Clone)]
pub struct GameAssetService {
    root: PathBuf,
    client: reqwest::Client,
    hunt_reference_client: reqwest::Client,
    downloads: Arc<Semaphore>,
    inflight: Arc<Mutex<HashMap<String, Arc<OnceCell<Result<ResolvedAsset, String>>>>>>,
    negative: Arc<Mutex<HashMap<String, Instant>>>,
    catalog: Arc<Mutex<Option<Arc<CatalogSource>>>>,
    sprites: Arc<Mutex<HashMap<u64, Option<PokemonSprite>>>>,
    hunt_reference: Arc<OnceCell<Arc<serde_json::Value>>>,
    hunt_species_looktypes: Arc<OnceCell<Arc<BTreeMap<u64, u64>>>>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedAsset {
    pub path: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GameItemCatalog {
    pub items: BTreeMap<String, CatalogItem>,
    pub marker_atlas: MarkerAtlas,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogItem {
    pub id: u64,
    pub name: String,
    pub category: Option<String>,
    pub asset_path: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkerAtlas {
    pub asset_path: String,
    pub cell: u32,
    pub cols: u32,
    pub rows: u32,
    pub slots: BTreeMap<String, [u32; 2]>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PokemonSprite {
    pub asset_path: String,
    pub x: u32,
    pub y: u32,
    pub width: u32,
    pub height: u32,
    pub page_width: u32,
    pub page_height: u32,
    // Atlas coordinates for the animation frames; the first frame retains
    // the legacy x/y/width/height API for React and older clients.
    pub frames: Vec<[u32; 4]>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogSource {
    items: BTreeMap<String, CatalogItem>,
    marker_atlas: MarkerAtlas,
    outfits: BTreeMap<String, OutfitIndexEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OutfitIndexEntry {
    manifest: String,
}

#[derive(Deserialize)]
struct ItemsDocument {
    items: Vec<SourceItem>,
}

#[derive(Deserialize)]
struct SourceItem {
    id: u64,
    name: String,
    category: Option<String>,
    icon: Option<String>,
}

#[derive(Deserialize)]
struct MarkerDocument {
    image: String,
    cell: u32,
    cols: u32,
    rows: u32,
    slots: BTreeMap<String, [u32; 2]>,
}

#[derive(Deserialize)]
struct OutfitsDocument {
    outfits: BTreeMap<String, OutfitIndexEntry>,
}

impl GameAssetService {
    pub fn new(app_data_dir: &Path) -> Result<Self, String> {
        let root = app_data_dir.join("assets");
        fs::create_dir_all(root.join("files")).map_err(|error| error.to_string())?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(4))
            .timeout(Duration::from_secs(12))
            .build()
            .map_err(|error| error.to_string())?;
        let hunt_reference_client = reqwest::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(4))
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= 3
                    || attempt.url().host_str() != Some("guiapokeidlehardtocapture.site")
                {
                    attempt.stop()
                } else {
                    attempt.follow()
                }
            }))
            .build()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            root,
            client,
            hunt_reference_client,
            // Asset requests are demand-driven. Six simultaneous transfers keep the
            // interface responsive without allowing an inventory grid to flood I/O.
            downloads: Arc::new(Semaphore::new(6)),
            inflight: Arc::new(Mutex::new(HashMap::new())),
            negative: Arc::new(Mutex::new(HashMap::new())),
            catalog: Arc::new(Mutex::new(None)),
            sprites: Arc::new(Mutex::new(HashMap::new())),
            hunt_reference: Arc::new(OnceCell::new()),
            hunt_species_looktypes: Arc::new(OnceCell::new()),
        })
    }

    pub async fn item_catalog(&self) -> Result<GameItemCatalog, String> {
        let source = self.catalog().await?;
        Ok(GameItemCatalog {
            items: source.items.clone(),
            marker_atlas: source.marker_atlas.clone(),
        })
    }

    /// Loads the public, read-only calculation reference used by the community
    /// hunt guide. The response is parsed as JSON only; its JavaScript is never
    /// evaluated, and the fixed host/version prevent arbitrary URL fetching.
    pub async fn where_to_hunt_reference(&self) -> Result<serde_json::Value, String> {
        let value = self
            .hunt_reference
            .get_or_try_init(|| async {
                let mut response = self
                    .hunt_reference_client
                    .get(format!("{HUNT_GUIDE_ORIGIN}/data.js?v=32"))
                    .send()
                    .await
                    .map_err(|error| {
                        format!("Não foi possível obter a referência de hunts: {error}")
                    })?
                    .error_for_status()
                    .map_err(|error| {
                        format!("A referência de hunts retornou status inesperado: {error}")
                    })?;
                if response
                    .content_length()
                    .is_some_and(|length| length > MAX_HUNT_REFERENCE_BYTES as u64)
                {
                    return Err("A referência de hunts excedeu o limite de tamanho.".to_owned());
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = response.chunk().await.map_err(|error| {
                    format!("Não foi possível ler a referência de hunts: {error}")
                })? {
                    if bytes.len().saturating_add(chunk.len()) > MAX_HUNT_REFERENCE_BYTES {
                        return Err("A referência de hunts excedeu o limite de tamanho.".to_owned());
                    }
                    bytes.extend_from_slice(&chunk);
                }
                let script = std::str::from_utf8(&bytes)
                    .map_err(|_| "A referência de hunts não está em UTF-8.".to_owned())?;
                let parsed = parse_hunt_reference_script(script)?;
                Ok::<_, String>(Arc::new(parsed))
            })
            .await?;
        let mut reference = (**value).clone();
        if let Ok(looktypes) = self.hunt_species_looktypes().await {
            reference["looktypes"] = serde_json::to_value(&*looktypes).map_err(|error| {
                format!("Não foi possível preparar os sprites das hunts: {error}")
            })?;
        }
        Ok(reference)
    }

    async fn hunt_species_looktypes(&self) -> Result<Arc<BTreeMap<u64, u64>>, String> {
        self.hunt_species_looktypes
            .get_or_try_init(|| async {
                let (base, current, outland, sprite_patches) = tokio::join!(
                    self.fetch_json::<serde_json::Value>("/assets/creatures.json"),
                    self.fetch_json::<serde_json::Value>("/assets/creatures-novos.json"),
                    self.fetch_json::<serde_json::Value>("/assets/creatures-outland-novos.json"),
                    self.fetch_json::<serde_json::Value>("/assets/creatures-sprites-lab.json"),
                );
                let base = base
                    .map_err(|error| format!("Catálogo base de sprites indisponível: {error}"))?;
                let current = current
                    .map_err(|error| format!("Catálogo atual de sprites indisponível: {error}"))?;
                let outland = outland.map_err(|error| {
                    format!("Catálogo de sprites de Outland indisponível: {error}")
                })?;
                let sprite_patches = sprite_patches
                    .map_err(|error| format!("Ajustes de sprites indisponíveis: {error}"))?;
                if [&base, &current, &outland].into_iter().any(|document| {
                    !document
                        .get("creatures")
                        .is_some_and(serde_json::Value::is_array)
                }) || !sprite_patches
                    .get("patches")
                    .is_some_and(serde_json::Value::is_array)
                {
                    return Err("Um dos catálogos de sprites está incompleto.".to_owned());
                }
                let mut looktypes = BTreeMap::new();
                for document in [base, current, outland] {
                    merge_species_looktypes(&mut looktypes, &document);
                }
                merge_sprite_looktype_patches(&mut looktypes, &sprite_patches);
                if looktypes.is_empty() {
                    return Err("O catálogo de sprites das espécies está indisponível.".to_owned());
                }
                Ok::<_, String>(Arc::new(looktypes))
            })
            .await
            .cloned()
    }

    pub async fn resolve_asset(&self, asset_path: String) -> Result<ResolvedAsset, String> {
        let asset_path = validate_asset_path(&asset_path)?;
        if let Some(until) = self.negative.lock().get(&asset_path).copied() {
            if Instant::now() < until {
                return Err("Recurso indisponível temporariamente.".into());
            }
            self.negative.lock().remove(&asset_path);
        }
        let cell = {
            let mut inflight = self.inflight.lock();
            inflight
                .entry(asset_path.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let result = cell
            .get_or_init(|| async {
                let permit = self
                    .downloads
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| "Fila de assets encerrada.".to_string())?;
                let value = self.download_asset(&asset_path).await;
                drop(permit);
                value
            })
            .await
            .clone();
        self.inflight.lock().remove(&asset_path);
        if result.is_err() {
            self.negative
                .lock()
                .insert(asset_path, Instant::now() + NEGATIVE_CACHE_FOR);
        }
        result
    }

    pub async fn pokemon_sprite(
        &self,
        looktypes: Vec<u64>,
    ) -> Result<Option<PokemonSprite>, String> {
        let catalog = self.catalog().await?;
        for looktype in looktypes {
            if let Some(cached) = self.sprites.lock().get(&looktype).cloned() {
                if cached.is_some() {
                    return Ok(cached);
                }
                continue;
            }
            let sprite = self.load_sprite(&catalog, looktype).await?;
            self.sprites.lock().insert(looktype, sprite.clone());
            if sprite.is_some() {
                return Ok(sprite);
            }
        }
        Ok(None)
    }

    async fn catalog(&self) -> Result<Arc<CatalogSource>, String> {
        if let Some(catalog) = self.catalog.lock().clone() {
            return Ok(catalog);
        }
        // Unlike individual images, a failed metadata refresh must not become a
        // process-lifetime failure. The next on-demand view is allowed to retry.
        let mut catalog = match self.fetch_catalog().await {
            Ok(catalog) => catalog,
            Err(network_error) => read_cached_catalog(&self.root).map_err(|_| network_error)?,
        };
        // Some first-party items intentionally live outside the mirror catalog.
        // Apply these confirmed client mappings to a stale disk cache too, then
        // persist the normalized result for an offline next start.
        apply_confirmed_client_item_extensions(&mut catalog.items);
        let _ = fs::write(
            self.root.join(CACHE_VERSION),
            serde_json::to_vec(&catalog).map_err(|error| error.to_string())?,
        );
        let catalog = Arc::new(catalog);
        *self.catalog.lock() = Some(catalog.clone());
        Ok(catalog)
    }

    async fn fetch_catalog(&self) -> Result<CatalogSource, String> {
        let (items, icons, atlas, outfits) = tokio::try_join!(
            self.fetch_json::<ItemsDocument>("/assets/items.json"),
            self.fetch_json::<BTreeMap<String, String>>("/assets/items-icons.json"),
            self.fetch_json::<MarkerDocument>("/assets/site/assets/maps/marker-atlas.json"),
            self.fetch_json::<OutfitsDocument>("/assets/asset-packs/outfits-index.json"),
        )?;
        let mut items: BTreeMap<String, CatalogItem> = items
            .items
            .into_iter()
            .map(|item| {
                let icon = icons
                    .get(&item.id.to_string())
                    .cloned()
                    .or_else(|| item.icon.as_deref().and_then(normalize_item_icon));
                (
                    item.id.to_string(),
                    CatalogItem {
                        id: item.id,
                        name: item.name,
                        category: item.category,
                        asset_path: icon.map(|path| format!("assets/{path}")),
                    },
                )
            })
            .collect();
        apply_confirmed_client_item_extensions(&mut items);
        Ok(CatalogSource {
            items,
            marker_atlas: MarkerAtlas {
                // The manifest adds a cache-busting query. The physical image is
                // immutable by path and the Manager stores it with a content key.
                asset_path: format!(
                    "assets/{}",
                    atlas
                        .image
                        .split('?')
                        .next()
                        .unwrap_or_default()
                        .trim_start_matches("/assets/")
                ),
                cell: atlas.cell,
                cols: atlas.cols,
                rows: atlas.rows,
                slots: atlas.slots,
            },
            outfits: outfits.outfits,
        })
    }

    async fn fetch_json<T: for<'a> Deserialize<'a>>(&self, path: &str) -> Result<T, String> {
        self.client
            .get(format!("{GAME_ORIGIN}{path}"))
            .send()
            .await
            .map_err(|error| format!("Não foi possível obter o catálogo: {error}"))?
            .error_for_status()
            .map_err(|error| format!("Catálogo retornou status inesperado: {error}"))?
            .json::<T>()
            .await
            .map_err(|error| format!("Catálogo inválido: {error}"))
    }

    async fn load_sprite(
        &self,
        catalog: &CatalogSource,
        looktype: u64,
    ) -> Result<Option<PokemonSprite>, String> {
        let Some(entry) = catalog.outfits.get(&looktype.to_string()) else {
            return Ok(None);
        };
        let manifest_path = entry
            .manifest
            .replace("/assets-packs/", "/assets/asset-packs/");
        let manifest: serde_json::Value = self.fetch_json(&manifest_path).await?;
        let Some((_, category)) = manifest
            .get("categories")
            .and_then(serde_json::Value::as_object)
            .and_then(|categories| categories.iter().next())
        else {
            return Ok(None);
        };
        let Some(page) = category
            .get("pages")
            .and_then(serde_json::Value::as_array)
            .and_then(|pages| pages.first())
        else {
            return Ok(None);
        };
        let Some(image) = page.get("image").and_then(serde_json::Value::as_str) else {
            return Ok(None);
        };
        let page_width = page
            .get("width")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        let page_height = page
            .get("height")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as u32;
        let Some((_, frame)) = manifest
            .get("assets")
            .and_then(serde_json::Value::as_object)
            .and_then(|assets| assets.iter().find(|(key, _)| key.ends_with("/1_1_1_3.png")))
            .and_then(|(_, asset)| {
                asset
                    .get("frames")
                    .and_then(serde_json::Value::as_array)
                    .and_then(|frames| frames.first())
                    .map(|frame| ((), frame))
            })
        else {
            return Ok(None);
        };
        let number = |key: &str| {
            frame
                .get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0) as u32
        };
        // The source game manifest contains the actual animation atlas
        // rectangles. Extract them rather than manufacturing frames from a
        // uniform grid. Cap the number kept in memory per Pokémon.
        let frames = manifest.get("assets")
            .and_then(serde_json::Value::as_object)
            .and_then(|assets| assets.iter().find(|(key, _)| key.ends_with("/1_1_1_3.png")))
            .and_then(|(_, asset)| asset.get("frames"))
            .and_then(serde_json::Value::as_array)
            .map(|frames| frames.iter().take(16).filter_map(|cell| {
                let read = |key| cell.get(key).and_then(serde_json::Value::as_u64)
                    .and_then(|value| u32::try_from(value).ok());
                let (x, y, w, h) = (read("x")?, read("y")?, read("w")?, read("h")?);
                (w > 0 && h > 0 && x.checked_add(w)? <= page_width
                    && y.checked_add(h)? <= page_height)
                    .then_some([x, y, w, h])
            }).collect::<Vec<_>>())
            .unwrap_or_default();
        let width = number("w");
        let height = number("h");
        if width == 0 || height == 0 || page_width == 0 || page_height == 0 {
            return Ok(None);
        }
        let asset_path = format!(
            "assets/{}",
            image
                .replace("/assets-packs/", "asset-packs/")
                .trim_start_matches('/'),
        );
        Ok(Some(PokemonSprite {
            asset_path,
            x: number("x"),
            y: number("y"),
            width,
            height,
            page_width,
            page_height,
            frames,
        }))
    }

    async fn download_asset(&self, asset_path: &str) -> Result<ResolvedAsset, String> {
        let filename = cache_filename(asset_path);
        let output = self.root.join("files").join(filename);
        if valid_cached_file(&output) {
            return Ok(ResolvedAsset {
                path: output.to_string_lossy().into_owned(),
            });
        }
        let response = self
            .client
            .get(format!("{GAME_ORIGIN}/{asset_path}"))
            .send()
            .await
            .map_err(|error| format!("Falha ao baixar recurso: {error}"))?
            .error_for_status()
            .map_err(|error| format!("Recurso retornou status inesperado: {error}"))?;
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if !content_type.starts_with("image/") {
            return Err("O recurso remoto não é uma imagem.".into());
        }
        if response
            .content_length()
            .is_some_and(|length| length as usize > MAX_ASSET_BYTES)
        {
            return Err("Imagem maior que o limite local.".into());
        }
        let bytes = response.bytes().await.map_err(|error| error.to_string())?;
        if bytes.len() > MAX_ASSET_BYTES || !valid_image_bytes(&bytes) {
            return Err("Imagem inválida ou maior que o limite local.".into());
        }
        let temporary = output.with_extension("part");
        fs::write(&temporary, &bytes).map_err(|error| error.to_string())?;
        fs::rename(&temporary, &output).map_err(|error| error.to_string())?;
        Ok(ResolvedAsset {
            path: output.to_string_lossy().into_owned(),
        })
    }
}

fn normalize_item_icon(icon: &str) -> Option<String> {
    if icon.starts_with("site/") {
        Some(icon.to_owned())
    } else if icon.starts_with('/') {
        Some(format!("site{icon}"))
    } else if !icon.is_empty() {
        Some(format!("site/assets/items/{icon}"))
    } else {
        None
    }
}

fn validate_asset_path(path: &str) -> Result<String, String> {
    let path = path.trim().trim_start_matches('/');
    let allowed_prefix = path.starts_with("assets/site/")
        || path.starts_with("assets/asset-packs/")
        || path.starts_with("img/");
    if !allowed_prefix
        || path.contains("..")
        || path.contains('?')
        || path.contains('#')
        || !path
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
    {
        return Err("Caminho de asset não permitido.".into());
    }
    let extension = path
        .rsplit('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !matches!(extension.as_str(), "png" | "gif" | "webp" | "jpg" | "jpeg") {
        return Err("Formato de asset não permitido.".into());
    }
    Ok(path.to_owned())
}

fn cache_filename(path: &str) -> String {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    let extension = path.rsplit('.').next().unwrap_or("img");
    format!("{:016x}.{extension}", hasher.finish())
}

/// Items maintained by the public game client, but intentionally absent from
/// the mirrored `items.json` / `items-icons.json` catalog. Keep each entry
/// traceable to the client source; do not turn unknown inventory IDs into this
/// list speculatively.
fn apply_confirmed_client_item_extensions(items: &mut BTreeMap<String, CatalogItem>) {
    // `Golden Potion` is supplied by the game's welcome extension and mapped
    // by the official client to the first-party image below.
    items.entry("70070".into()).or_insert(CatalogItem {
        id: 70070,
        name: "Golden Potion".into(),
        category: Some("heal".into()),
        asset_path: Some("img/itens/golden-potion.png".into()),
    });
    // Confirmed in the official client as `item.fragmentoChave`; the public
    // first-party sprite is intentionally outside the mirrored item catalog.
    items.entry("70011".into()).or_insert(CatalogItem {
        id: 70011,
        name: "Fragmento de Chave".into(),
        category: Some("fragment".into()),
        asset_path: Some("img/itens/fragmento-chave.png".into()),
    });
    // Confirmed in the official client's NOSSOS_ITENS_I18N as
    // `item.fragmentoBicicleta`; the Portuguese translation is
    // “Fragmento de Bicicleta”. Its first-party PNG is public but absent from
    // the mirrored item icon list.
    items.entry("70013".into()).or_insert(CatalogItem {
        id: 70013,
        name: "Fragmento de Bicicleta".into(),
        category: Some("fragment".into()),
        asset_path: Some("img/itens/fragmento-bicicleta.png".into()),
    });
    // The public client keeps the Shiny Stone fragment outside the mirror too.
    // Its official workbench artwork is the first-party Shiny Stone image below.
    items.entry("70012".into()).or_insert(CatalogItem {
        id: 70012,
        name: "Fragmento de Shiny Stone".into(),
        category: Some("fragment".into()),
        asset_path: Some("img/itens/shiny-stone.png".into()),
    });
    // Confirmed from the live Community Market: the Rock variant is item
    // 70032 and intentionally reuses the Shiny Stone fragment artwork.
    items.entry("70032".into()).or_insert(CatalogItem {
        id: 70032,
        name: "Shiny Stone ROCK".into(),
        category: Some("stone".into()),
        asset_path: Some("img/itens/shiny-stone.png".into()),
    });
    // The two MEGA fragments are named and rendered by the game client itself,
    // but omitted from both public mirror files.
    items.entry("70014".into()).or_insert(CatalogItem {
        id: 70014,
        name: "Fragmento de MEGA Stone".into(),
        category: Some("fragment".into()),
        asset_path: Some("img/itens/fragmento-mega.png".into()),
    });
    items.entry("70015".into()).or_insert(CatalogItem {
        id: 70015,
        name: "Fragmento de MEGA Shiny Stone".into(),
        category: Some("fragment".into()),
        asset_path: Some("img/itens/fragmento-mega-shiny.png".into()),
    });
    // Houses, founder boxes, and bicycles are inventory entries in the live
    // game, yet their metadata is intentionally assembled by the client rather
    // than included in the generic item catalog.
    items.entry("70040".into()).or_insert(CatalogItem {
        id: 70040,
        name: "Casa Comum".into(),
        category: Some("house".into()),
        asset_path: Some("img/itens/casa-comum.png".into()),
    });
    items.entry("70041".into()).or_insert(CatalogItem {
        id: 70041,
        name: "Casa Incomum".into(),
        category: Some("house".into()),
        asset_path: Some("img/itens/casa-incomum.png".into()),
    });
    items.entry("70060".into()).or_insert(CatalogItem {
        id: 70060,
        name: "Caixa de Fundador".into(),
        category: Some("box".into()),
        asset_path: Some("img/itens/caixa-fundador.png".into()),
    });
    items.entry("70061".into()).or_insert(CatalogItem {
        id: 70061,
        name: "Caixa de CoFundador".into(),
        category: Some("box".into()),
        asset_path: Some("img/itens/caixa-cofundador.png".into()),
    });
    items.entry("70080".into()).or_insert(CatalogItem {
        id: 70080,
        name: "Bicicleta Comum".into(),
        category: Some("bicycle".into()),
        asset_path: Some("img/itens/bicicleta-comum.png".into()),
    });
}

fn valid_cached_file(path: &Path) -> bool {
    fs::read(path)
        .map(|bytes| {
            !bytes.is_empty() && bytes.len() <= MAX_ASSET_BYTES && valid_image_bytes(&bytes)
        })
        .unwrap_or(false)
}

fn valid_image_bytes(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || bytes.starts_with(b"GIF87a")
        || bytes.starts_with(b"GIF89a")
        || (bytes.len() >= 12 && &bytes[0..4] == b"RIFF" && &bytes[8..12] == b"WEBP")
        || bytes.starts_with(&[0xff, 0xd8, 0xff])
}

fn read_cached_catalog(root: &Path) -> Result<CatalogSource, String> {
    serde_json::from_slice(&fs::read(root.join(CACHE_VERSION)).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_public_reference_as_json_without_evaluating_its_script() {
        let parsed = parse_hunt_reference_script(
            "// generated reference\nwindow.PI_DATA = {\"especies\":[],\"hunts\":[]};\n",
        )
        .unwrap();
        assert_eq!(parsed["especies"].as_array().unwrap().len(), 0);
        assert_eq!(parsed["hunts"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn rejects_unexpected_hunt_reference_payloads() {
        assert!(parse_hunt_reference_script("window.PI_DATA = alert('no');").is_err());
        assert!(parse_hunt_reference_script("window.PI_DATA = {\"hunts\":[]};").is_err());
    }

    #[test]
    fn merges_species_looktypes_and_applies_sprite_patches_last() {
        let mut looktypes = BTreeMap::new();
        merge_species_looktypes(
            &mut looktypes,
            &serde_json::json!({ "creatures": [
                { "pokeId": 292, "looktype": 60292 },
                { "pokeId": 0, "looktype": 60000 },
                { "pokeId": 339, "looktype": "60339" },
                { "pokeId": 25, "looktype": 1 }
            ]}),
        );
        merge_sprite_looktype_patches(
            &mut looktypes,
            &serde_json::json!({ "patches": [
                { "pokeId": 292, "looktype": 70292 },
                { "pokeId": 339, "looktype": 0 }
            ]}),
        );

        assert_eq!(looktypes.get(&292), Some(&60292));
        assert!(!looktypes.contains_key(&339));
        assert!(!looktypes.contains_key(&25));
    }

    #[test]
    fn only_accepts_known_public_asset_paths() {
        assert!(validate_asset_path("assets/site/assets/items/air_tank.png").is_ok());
        assert!(validate_asset_path("assets/asset-packs/outfits/male/401/a.webp").is_ok());
        assert!(validate_asset_path("img/itens/golden-potion.png").is_ok());
        assert!(validate_asset_path("https://example.test/nope.png").is_err());
        assert!(validate_asset_path("assets/site/../secret.png").is_err());
        assert!(validate_asset_path("assets/site/assets/file.svg").is_err());
    }

    #[test]
    fn validates_supported_image_signatures() {
        assert!(valid_image_bytes(b"\x89PNG\r\n\x1a\nrest"));
        assert!(valid_image_bytes(b"GIF89arest"));
        assert!(valid_image_bytes(b"RIFFxxxxWEBPrest"));
        assert!(!valid_image_bytes(b"<html>not an image</html>"));
    }

    #[test]
    fn cache_key_is_stable_without_exposing_remote_names() {
        let first = cache_filename("assets/site/assets/items/air_tank.png");
        assert_eq!(
            first,
            cache_filename("assets/site/assets/items/air_tank.png")
        );
        assert_ne!(
            first,
            cache_filename("assets/site/assets/items/bat_wing.png")
        );
        assert!(!first.contains("air_tank"));
    }

    #[test]
    fn adds_the_confirmed_client_item_extensions_missing_from_the_mirror_catalog() {
        let mut items = BTreeMap::new();
        apply_confirmed_client_item_extensions(&mut items);
        let key_fragment = items.get("70011").expect("key fragment");
        assert_eq!(key_fragment.name, "Fragmento de Chave");
        assert_eq!(
            key_fragment.asset_path.as_deref(),
            Some("img/itens/fragmento-chave.png")
        );
        let fragment = items.get("70013").expect("bicycle fragment");
        assert_eq!(fragment.name, "Fragmento de Bicicleta");
        assert_eq!(
            fragment.asset_path.as_deref(),
            Some("img/itens/fragmento-bicicleta.png")
        );
        for (id, name, asset_path) in [
            (
                "70012",
                "Fragmento de Shiny Stone",
                "img/itens/shiny-stone.png",
            ),
            ("70032", "Shiny Stone ROCK", "img/itens/shiny-stone.png"),
            (
                "70014",
                "Fragmento de MEGA Stone",
                "img/itens/fragmento-mega.png",
            ),
            (
                "70015",
                "Fragmento de MEGA Shiny Stone",
                "img/itens/fragmento-mega-shiny.png",
            ),
            ("70040", "Casa Comum", "img/itens/casa-comum.png"),
            ("70041", "Casa Incomum", "img/itens/casa-incomum.png"),
            ("70060", "Caixa de Fundador", "img/itens/caixa-fundador.png"),
            (
                "70061",
                "Caixa de CoFundador",
                "img/itens/caixa-cofundador.png",
            ),
            ("70080", "Bicicleta Comum", "img/itens/bicicleta-comum.png"),
        ] {
            let item = items.get(id).unwrap_or_else(|| panic!("missing item {id}"));
            assert_eq!(item.name, name);
            assert_eq!(item.asset_path.as_deref(), Some(asset_path));
        }
    }
}
