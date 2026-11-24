//! API keys for the public listener.
//!
//! Every request on the public port needs `Authorization: Bearer <key>`. A key
//! carries scopes: `core` for `/v1/*`, a plugin name for `/plugins/{name}/*`,
//! or `*` for everything, plugins installed later included. Keys are managed
//! only on the admin listener; plugins and nodes keep their own credentials.

use axum::{
    Json,
    extract::{Path as AxumPath, Request, State},
    http::{HeaderValue, Method, StatusCode, header::AUTHORIZATION, header::WWW_AUTHENTICATE},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::api::{ApiResult, App, error, now_ms, same_hash, save, secret, secret_hash};

/// Scope for the core API.
pub const CORE_SCOPE: &str = "core";
/// Scope that grants every API, plugins installed later included.
pub const ALL_SCOPE: &str = "*";
const KEY_PREFIX: &str = "sr_";
/// Characters of a key kept in plain text so people can tell keys apart.
const SHOWN_PREFIX: usize = KEY_PREFIX.len() + 8;
/// `last_used_at_ms` is written at most this often per key.
const LAST_USED_RESOLUTION_MS: u64 = 60_000;
const MAX_NAME: usize = 64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: Uuid,
    pub name: String,
    pub prefix: String,
    pub key_hash: String,
    pub scopes: Vec<String>,
    pub enabled: bool,
    pub created_at_ms: u64,
    #[serde(default)]
    pub expires_at_ms: Option<u64>,
    #[serde(default)]
    pub last_used_at_ms: Option<u64>,
    /// Hash of the key before the last rotation, accepted until `previous_expires_at_ms`.
    #[serde(default)]
    pub previous_hash: Option<String>,
    #[serde(default)]
    pub previous_expires_at_ms: Option<u64>,
}

impl ApiKey {
    fn matches(&self, hash: &str, now: u64) -> bool {
        same_hash(&self.key_hash, hash)
            || self.previous_hash.as_deref().is_some_and(|previous| {
                self.previous_expires_at_ms.is_some_and(|until| until > now)
                    && same_hash(previous, hash)
            })
    }

    fn allows(&self, scope: &str) -> bool {
        self.scopes
            .iter()
            .any(|granted| granted == ALL_SCOPE || granted == scope)
    }
}

/// The name of the key a public request authenticated with.
#[derive(Clone)]
pub struct KeyName(pub String);

/// The scope a public request path needs, or `None` when it needs no key.
fn required_scope(path: &str) -> Option<String> {
    if path == "/health" || path.starts_with("/internal/") {
        return None;
    }
    match path.strip_prefix("/plugins/") {
        Some(rest) => Some(rest.split('/').next().unwrap_or_default().to_owned()),
        None => Some(CORE_SCOPE.to_owned()),
    }
}

fn denied(status: StatusCode, message: &str) -> Response {
    let mut response = error(status, message).into_response();
    if status == StatusCode::UNAUTHORIZED {
        response
            .headers_mut()
            .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    response
}

/// Middleware for the public listener: check the bearer key against the path's scope.
pub async fn require_key(State(app): State<App>, mut request: Request, next: Next) -> Response {
    let Some(scope) = required_scope(request.uri().path()) else {
        return next.run(request).await;
    };
    let Some(token) = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|token| token.trim().to_owned())
        .filter(|token| !token.is_empty())
    else {
        return denied(
            StatusCode::UNAUTHORIZED,
            "an API key is required: Authorization: Bearer <key>",
        );
    };
    let hash = secret_hash(&token);
    let now = now_ms();
    let (id, name, touch) = {
        let cluster = app.cluster.read().await;
        // Nodes read the cluster's images, disks, pools and volumes with their own credential.
        if scope == CORE_SCOPE
            && matches!(*request.method(), Method::GET | Method::HEAD)
            && cluster
                .node_credentials
                .values()
                .any(|expected| same_hash(expected, &hash))
        {
            drop(cluster);
            return next.run(request).await;
        }
        let Some(key) = cluster
            .api_keys
            .values()
            .find(|key| key.matches(&hash, now))
        else {
            return denied(StatusCode::UNAUTHORIZED, "unknown API key");
        };
        if !key.enabled {
            return denied(StatusCode::UNAUTHORIZED, "API key is disabled");
        }
        if key.expires_at_ms.is_some_and(|expires| expires <= now) {
            return denied(StatusCode::UNAUTHORIZED, "API key has expired");
        }
        if !key.allows(&scope) {
            let message = if scope == CORE_SCOPE {
                "API key does not grant the core API".to_owned()
            } else {
                format!("API key does not grant plugin {scope}")
            };
            return denied(StatusCode::FORBIDDEN, &message);
        }
        let touch = key
            .last_used_at_ms
            .is_none_or(|last| now.saturating_sub(last) >= LAST_USED_RESOLUTION_MS);
        (key.id, key.name.clone(), touch)
    };
    if touch {
        let mut cluster = app.cluster.write().await;
        if let Some(key) = cluster.api_keys.get_mut(&id) {
            key.last_used_at_ms = Some(now);
            if let Err((_, Json(cause))) = save(&app, &cluster).await {
                tracing::warn!(%cause, "could not record API key use");
            }
        }
    }
    request.headers_mut().remove(AUTHORIZATION);
    request.extensions_mut().insert(KeyName(name));
    next.run(request).await
}

#[derive(Serialize)]
pub struct ApiKeyView {
    id: Uuid,
    name: String,
    prefix: String,
    scopes: Vec<String>,
    enabled: bool,
    created_at_ms: u64,
    expires_at_ms: Option<u64>,
    last_used_at_ms: Option<u64>,
    previous_expires_at_ms: Option<u64>,
    /// The full key, returned only when it is created or rotated.
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<String>,
}

impl ApiKeyView {
    fn new(key: &ApiKey, now: u64, plain: Option<String>) -> Self {
        Self {
            id: key.id,
            name: key.name.clone(),
            prefix: key.prefix.clone(),
            scopes: key.scopes.clone(),
            enabled: key.enabled,
            created_at_ms: key.created_at_ms,
            expires_at_ms: key.expires_at_ms,
            last_used_at_ms: key.last_used_at_ms,
            previous_expires_at_ms: key.previous_expires_at_ms.filter(|until| *until > now),
            key: plain,
        }
    }
}

fn new_secret() -> (String, String, String) {
    let plain = format!("{KEY_PREFIX}{}", secret());
    let prefix = plain[..SHOWN_PREFIX].to_owned();
    let hash = secret_hash(&plain);
    (plain, prefix, hash)
}

fn clean_name(name: &str) -> ApiResult<String> {
    let name = name.trim();
    if name.is_empty() || name.chars().count() > MAX_NAME {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("key name must be 1-{MAX_NAME} characters"),
        ));
    }
    Ok(name.to_owned())
}

/// Validate scopes; `kept` are scopes the key already had and may keep even
/// when their plugin is gone.
fn clean_scopes(
    app_plugins: &[String],
    scopes: &[String],
    kept: &[String],
) -> ApiResult<Vec<String>> {
    let mut result = Vec::new();
    for scope in scopes {
        let scope = scope.trim();
        let known = scope == ALL_SCOPE
            || scope == CORE_SCOPE
            || app_plugins.iter().any(|plugin| plugin == scope)
            || kept.iter().any(|old| old == scope);
        if !known {
            return Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("unknown scope {scope}: use *, core, or an installed plugin name"),
            ));
        }
        if !result.iter().any(|existing| existing == scope) {
            result.push(scope.to_owned());
        }
    }
    if result.is_empty() {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "a key needs at least one scope",
        ));
    }
    if result.iter().any(|scope| scope == ALL_SCOPE) {
        result = vec![ALL_SCOPE.to_owned()];
    }
    Ok(result)
}

fn sorted(mut keys: Vec<ApiKeyView>) -> Vec<ApiKeyView> {
    keys.sort_by(|left, right| {
        left.created_at_ms
            .cmp(&right.created_at_ms)
            .then(left.id.cmp(&right.id))
    });
    keys
}

pub async fn list_keys(State(app): State<App>) -> Json<Vec<ApiKeyView>> {
    let now = now_ms();
    let cluster = app.cluster.read().await;
    Json(sorted(
        cluster
            .api_keys
            .values()
            .map(|key| ApiKeyView::new(key, now, None))
            .collect(),
    ))
}

#[derive(Deserialize)]
pub struct CreateKey {
    name: String,
    scopes: Vec<String>,
    #[serde(default)]
    expires_at_ms: Option<u64>,
}

pub async fn create_key(
    State(app): State<App>,
    Json(request): Json<CreateKey>,
) -> ApiResult<Json<ApiKeyView>> {
    let now = now_ms();
    let name = clean_name(&request.name)?;
    if request.expires_at_ms.is_some_and(|expires| expires <= now) {
        return Err(error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "expires_at_ms must be in the future",
        ));
    }
    let mut cluster = app.cluster.write().await;
    let plugins = cluster.plugins.keys().cloned().collect::<Vec<_>>();
    let scopes = clean_scopes(&plugins, &request.scopes, &[])?;
    let (plain, prefix, key_hash) = new_secret();
    let key = ApiKey {
        id: Uuid::new_v4(),
        name,
        prefix,
        key_hash,
        scopes,
        enabled: true,
        created_at_ms: now,
        expires_at_ms: request.expires_at_ms,
        last_used_at_ms: None,
        previous_hash: None,
        previous_expires_at_ms: None,
    };
    let view = ApiKeyView::new(&key, now, Some(plain));
    cluster.api_keys.insert(key.id, key);
    save(&app, &cluster).await?;
    Ok(Json(view))
}

#[derive(Deserialize)]
pub struct UpdateKey {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    scopes: Option<Vec<String>>,
    #[serde(default)]
    enabled: Option<bool>,
    /// `null` removes the expiry; leave the field out to keep it.
    #[serde(default, deserialize_with = "present")]
    expires_at_ms: Option<Option<u64>>,
}

/// Tells a field set to `null` apart from a missing one.
fn present<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Option<u64>>, D::Error> {
    Option::<u64>::deserialize(deserializer).map(Some)
}

pub async fn update_key(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
    Json(request): Json<UpdateKey>,
) -> ApiResult<Json<ApiKeyView>> {
    let now = now_ms();
    let mut cluster = app.cluster.write().await;
    let plugins = cluster.plugins.keys().cloned().collect::<Vec<_>>();
    let key = cluster
        .api_keys
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "API key not found"))?;
    if let Some(name) = &request.name {
        key.name = clean_name(name)?;
    }
    if let Some(scopes) = &request.scopes {
        key.scopes = clean_scopes(&plugins, scopes, &key.scopes)?;
    }
    if let Some(enabled) = request.enabled {
        key.enabled = enabled;
    }
    if let Some(expires) = request.expires_at_ms {
        if expires.is_some_and(|expires| expires <= now) {
            return Err(error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "expires_at_ms must be in the future",
            ));
        }
        key.expires_at_ms = expires;
    }
    let view = ApiKeyView::new(key, now, None);
    save(&app, &cluster).await?;
    Ok(Json(view))
}

#[derive(Deserialize, Default)]
pub struct RotateKey {
    /// Minutes the old key keeps working; 0 retires it at once.
    #[serde(default)]
    grace_minutes: u32,
}

pub async fn rotate_key(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
    request: Option<Json<RotateKey>>,
) -> ApiResult<Json<ApiKeyView>> {
    let grace = request.map(|Json(request)| request).unwrap_or_default();
    let now = now_ms();
    let mut cluster = app.cluster.write().await;
    let key = cluster
        .api_keys
        .get_mut(&id)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "API key not found"))?;
    let (plain, prefix, key_hash) = new_secret();
    if grace.grace_minutes > 0 {
        key.previous_hash = Some(std::mem::replace(&mut key.key_hash, key_hash));
        key.previous_expires_at_ms = Some(now + u64::from(grace.grace_minutes) * 60_000);
    } else {
        key.key_hash = key_hash;
        key.previous_hash = None;
        key.previous_expires_at_ms = None;
    }
    key.prefix = prefix;
    let view = ApiKeyView::new(key, now, Some(plain));
    save(&app, &cluster).await?;
    Ok(Json(view))
}

pub async fn delete_key(
    State(app): State<App>,
    AxumPath(id): AxumPath<Uuid>,
) -> ApiResult<StatusCode> {
    let mut cluster = app.cluster.write().await;
    if cluster.api_keys.remove(&id).is_none() {
        return Err(error(StatusCode::NOT_FOUND, "API key not found"));
    }
    save(&app, &cluster).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(scopes: &[&str]) -> ApiKey {
        ApiKey {
            id: Uuid::new_v4(),
            name: "test".into(),
            prefix: "sr_00000000".into(),
            key_hash: secret_hash("current"),
            scopes: scopes.iter().map(|scope| scope.to_string()).collect(),
            enabled: true,
            created_at_ms: 0,
            expires_at_ms: None,
            last_used_at_ms: None,
            previous_hash: Some(secret_hash("old")),
            previous_expires_at_ms: Some(1_000),
        }
    }

    #[test]
    fn scopes_follow_the_path() {
        assert_eq!(required_scope("/health"), None);
        assert_eq!(required_scope("/internal/join"), None);
        assert_eq!(required_scope("/v1/pools").as_deref(), Some("core"));
        assert_eq!(
            required_scope("/plugins/proxies/api/proxies").as_deref(),
            Some("proxies")
        );
        assert_eq!(required_scope("/nope").as_deref(), Some("core"));
    }

    #[test]
    fn keys_grant_their_scopes_and_the_wildcard_grants_all() {
        assert!(key(&["core"]).allows("core"));
        assert!(!key(&["core"]).allows("proxies"));
        assert!(key(&["proxies", "python"]).allows("python"));
        assert!(key(&["*"]).allows("anything-installed-later"));
    }

    #[test]
    fn previous_key_works_only_during_the_grace_period() {
        let key = key(&["core"]);
        assert!(key.matches(&secret_hash("current"), 5_000));
        assert!(key.matches(&secret_hash("old"), 999));
        assert!(!key.matches(&secret_hash("old"), 1_000));
        assert!(!key.matches(&secret_hash("other"), 0));
    }

    #[test]
    fn scope_validation_rejects_unknown_plugins_but_keeps_old_ones() {
        let plugins = vec!["proxies".to_owned()];
        assert!(clean_scopes(&plugins, &["queue".into()], &[]).is_err());
        assert!(clean_scopes(&plugins, &[], &[]).is_err());
        assert_eq!(
            clean_scopes(&plugins, &["queue".into()], &["queue".into()]).unwrap(),
            vec!["queue"]
        );
        assert_eq!(
            clean_scopes(&plugins, &["core".into(), "*".into()], &[]).unwrap(),
            vec!["*"]
        );
        assert_eq!(
            clean_scopes(
                &plugins,
                &["core".into(), "proxies".into(), "core".into()],
                &[]
            )
            .unwrap(),
            vec!["core", "proxies"]
        );
    }

    #[test]
    fn new_keys_are_prefixed_and_only_hashed() {
        let (plain, prefix, hash) = new_secret();
        assert!(plain.starts_with("sr_"));
        assert_eq!(prefix, &plain[..SHOWN_PREFIX]);
        assert_eq!(hash, secret_hash(&plain));
        assert_ne!(hash, plain);
    }
}
