//! Configuration for the `gabiond` binary.
//!
//! Every scalar setting is a row of the library's table
//! ([`gabion::settings::Setting`]), which names it, parses it and validates
//! it for both adapters. For each row the loader reads, in order of
//! precedence:
//!
//! 1. its `GABION_*` environment variable (a list setting splits on commas),
//! 2. else its dotted key in the optional YAML file,
//! 3. else the built-in default,
//!
//! and passes the text to [`Settings::apply`]. The YAML tree is walked once:
//! a scalar under a list setting counts as a one-item list, and a value of a
//! shape its setting cannot take (a table, a misplaced list, a fraction)
//! fails. A YAML key the table and a `limits:` entry do not know stops
//! startup, as does any value that fails to parse or validate, before gabiond
//! binds a socket.
//!
//! The `limits` list, where each entry is itself a struct with nested fields
//! and durations, cannot be expressed through environment variables and must
//! come from the YAML file.

use std::path::Path;
use std::time::Duration;

use config::{Config, File, FileFormat, Map, Value, ValueKind};
use serde::Deserialize;
use thiserror::Error;

use gabion::crdt::ValidStorageConfig;
use gabion::gossip::ValidGossipConfig;
use gabion::rules::{EnforcementMode, RuleDef, RuleError, RuleTable};
use gabion::settings::{Setting, SettingError, Settings};

/// The loaded configuration: every setting, validated once, and the rules.
#[derive(Clone, Debug)]
pub struct AppConfig {
    /// The values as loaded, for what the validated configs do not carry:
    /// the Envoy and admin binds, the node id seed, and the startup line.
    pub settings: Settings,
    pub gossip: ValidGossipConfig,
    pub storage: ValidStorageConfig,
    /// The `limits:` entries, each built through the library's one rule
    /// constructor, in declaration order.
    pub rules: RuleTable,
}

/// The fields of one `limits:` entry, and of one of its descriptors.
const LIMIT_FIELDS: [&str; 7] = [
    "name",
    "domain",
    "descriptors",
    "rate",
    "window",
    "bucket",
    "mode",
];
const DESCRIPTOR_FIELDS: [&str; 2] = ["key", "value"];

impl AppConfig {
    /// Load the environment over the optional YAML file over the defaults,
    /// and validate the result. If `yaml_path` is `None`, the server is
    /// configured purely from the environment and the defaults.
    pub fn load(yaml_path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut builder = Config::builder();
        if let Some(path) = yaml_path {
            builder = builder.add_source(File::new(&path.to_string_lossy(), FileFormat::Yaml));
        }
        Self::from_yaml(builder.build().map_err(ConfigError::Config)?)
    }

    /// Test-only loader that takes inline YAML instead of a file path.
    /// Same environment layering as [`Self::load`].
    #[cfg(test)]
    fn load_with_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let builder = Config::builder().add_source(File::from_str(yaml, FileFormat::Yaml));
        Self::from_yaml(builder.build().map_err(ConfigError::Config)?)
    }

    fn from_yaml(yaml: Config) -> Result<Self, ConfigError> {
        let mut root = yaml.cache.into_table().map_err(ConfigError::Config)?;
        let limits: Vec<LimitRuleConfig> = match root.remove("limits") {
            None => Vec::new(),
            Some(limits) => {
                if let ValueKind::Array(entries) = &limits.kind
                    && let Some(key) = entries.iter().enumerate().find_map(|(i, entry)| {
                        unknown_fields(entry, &LIMIT_FIELDS, &format!("limits[{i}]"))
                    })
                {
                    return Err(ConfigError::UnknownKey(key));
                }
                limits.try_deserialize().map_err(ConfigError::Config)?
            }
        };
        let mut settings = Settings::default();
        apply_yaml(&mut settings, root, "")?;
        for &setting in Setting::ALL {
            let Some(raw) = read_env(setting.env_var())? else {
                continue;
            };
            if setting.is_list() {
                for item in raw.split(',').filter(|item| !item.trim().is_empty()) {
                    settings.apply(setting, item)?;
                }
            } else {
                settings.apply(setting, &raw)?;
            }
        }
        let (gossip, storage) = settings.validate()?;
        let rules = limits
            .iter()
            .map(|limit| {
                let patterns: Box<[(&str, &str)]> = limit
                    .descriptors
                    .iter()
                    .map(|d| (&*d.key, &*d.value))
                    .collect();
                RuleDef {
                    name: &limit.name,
                    domain: &limit.domain,
                    patterns: &patterns,
                    rate: &limit.rate,
                    window: limit.window,
                    bucket: limit.bucket,
                    mode: limit.mode.into(),
                }
                .build(&storage.cardinality)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            settings,
            gossip,
            storage,
            rules: RuleTable::new(rules, storage.rule_dictionary_capacity())?,
        })
    }
}

/// Apply every setting under `table`, at dotted path `prefix`, whose
/// environment variable is unset. A key that is neither a setting nor a block
/// of settings fails, as does a value of a shape its setting cannot take.
fn apply_yaml(
    settings: &mut Settings,
    table: Map<String, Value>,
    prefix: &str,
) -> Result<(), ConfigError> {
    for (key, value) in table {
        let path = if prefix.is_empty() {
            key
        } else {
            format!("{prefix}.{key}")
        };
        if let Some(&setting) = Setting::ALL.iter().find(|s| s.yaml_key() == path) {
            // The environment variable wins; the env pass applies it.
            if std::env::var_os(setting.env_var()).is_some() {
                continue;
            }
            let items = match value.kind {
                ValueKind::Array(items) if setting.is_list() => items,
                ValueKind::Array(_) => {
                    return Err(ConfigError::WrongShape {
                        setting,
                        found: "a list",
                    });
                }
                _ => vec![value],
            };
            for item in items {
                let found = match item.kind {
                    ValueKind::Nil => continue,
                    ValueKind::Float(_) => "a fractional number",
                    ValueKind::Table(_) => "a table",
                    ValueKind::Array(_) => "a list",
                    _ => {
                        settings
                            .apply(setting, &item.into_string().map_err(ConfigError::Config)?)?;
                        continue;
                    }
                };
                return Err(ConfigError::WrongShape { setting, found });
            }
            continue;
        }
        match value.kind {
            ValueKind::Table(inner) => apply_yaml(settings, inner, &path)?,
            ValueKind::Nil
                if Setting::ALL.iter().any(|s| {
                    s.yaml_key()
                        .strip_prefix(path.as_str())
                        .is_some_and(|rest| rest.starts_with('.'))
                }) => {}
            _ => return Err(ConfigError::UnknownKey(path)),
        }
    }
    Ok(())
}

/// The first field of the table `value` not in `known`, as `path.field`, or
/// of one of its `descriptors`.
fn unknown_fields(value: &Value, known: &[&str], path: &str) -> Option<String> {
    let ValueKind::Table(fields) = &value.kind else {
        return None;
    };
    fields.iter().find_map(|(field, value)| match &value.kind {
        _ if !known.contains(&field.as_str()) => Some(format!("{path}.{field}")),
        ValueKind::Array(descriptors) if field == "descriptors" => {
            descriptors.iter().enumerate().find_map(|(j, descriptor)| {
                unknown_fields(
                    descriptor,
                    &DESCRIPTOR_FIELDS,
                    &format!("{path}.descriptors[{j}]"),
                )
            })
        }
        _ => None,
    })
}

/// Read an env var, distinguishing unset (`Ok(None)`) from non-UTF-8 (error).
fn read_env(name: &'static str) -> Result<Option<String>, ConfigError> {
    match std::env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(ConfigError::NonUtf8EnvVar(name)),
    }
}

#[derive(Debug, Deserialize)]
struct LimitRuleConfig {
    name: Box<str>,
    domain: Box<str>,
    descriptors: Box<[DescriptorConfig]>,
    /// Operator-facing rate: `N` requests per period, e.g. `10r/s`,
    /// `100r/5m`, `1000r/h`. Mandatory. See `gabion::rules::parse_rate`.
    rate: Box<str>,
    /// Optional window the rate is enforced over. Defaults to the rate's
    /// period when omitted. When set, the resolved internal limit scales
    /// up to `floor(rate_count * window_millis / period_millis)` — see
    /// the README's "Rate, window, and bucket" section.
    #[serde(default, with = "humantime_serde::option")]
    window: Option<Duration>,
    /// `bucket:` defaults to `window:` when omitted — one fixed-window
    /// bucket per rule. Set explicitly for finer sliding-window
    /// enforcement (e.g. `bucket: 1s` inside a `window: 60s` rule).
    #[serde(default, with = "humantime_serde::option")]
    bucket: Option<Duration>,
    #[serde(default)]
    mode: EnforcementModeConfig,
}

#[derive(Debug, Deserialize)]
struct DescriptorConfig {
    key: Box<str>,
    #[serde(default = "any_value")]
    value: Box<str>,
}

fn any_value() -> Box<str> {
    Box::from("*")
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
enum EnforcementModeConfig {
    #[default]
    Enforce,
    DryRun,
    Disabled,
}

impl From<EnforcementModeConfig> for EnforcementMode {
    fn from(value: EnforcementModeConfig) -> Self {
        match value {
            EnforcementModeConfig::Enforce => EnforcementMode::Enforce,
            EnforcementModeConfig::DryRun => EnforcementMode::DryRun,
            EnforcementModeConfig::Disabled => EnforcementMode::Disabled,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(transparent)]
    Setting(#[from] SettingError),
    #[error(transparent)]
    Rule(#[from] RuleError),
    #[error(
        "The configuration file sets `{0}`, which gabiond does not know. This key was removed or \
         renamed in this release, or it is misspelled; see the release notes for its replacement."
    )]
    UnknownKey(String),
    /// A YAML value of a shape its setting cannot take.
    #[error(
        "The configuration file sets {setting} to {found}, which gabiond cannot read as its \
         value. Usually the value is indented under the wrong key, or it is a number above \
         9223372036854775807 left unquoted, which YAML reads as a fraction and rounds. Correct \
         it, quoting a large number, or remove it to use the default."
    )]
    WrongShape {
        setting: Setting,
        found: &'static str,
    },
    #[error(
        "gabiond could not read its configuration: {0}. Settings come from built-in defaults, \
         then the YAML file given as the first argument, then `GABION_*` environment variables, \
         which win over the file, so the value may come from either. See \"How configuration \
         layers\" in the gabiond README."
    )]
    Config(#[source] config::ConfigError),
    #[error(
        "gabiond cannot start: it has no gossip address to listen on, and other gabion nodes need \
         one to reach it. Set `gossip.bind` in the YAML file (for example `0.0.0.0:9000`) or \
         export `GABION_GOSSIP_BIND`."
    )]
    MissingGossipBind,
    #[error(
        "`{0}` contains bytes that are not valid UTF-8, which gabiond cannot read. Set it again \
         as plain text."
    )]
    NonUtf8EnvVar(&'static str),
}

#[cfg(test)]
mod tests;
