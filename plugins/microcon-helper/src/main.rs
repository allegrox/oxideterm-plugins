// SPDX-License-Identifier: MIT
//! MicroCon Helper plugin runtime (Rust port of the original Node implementation).
//!
//! Speaks the OxideTerm process-plugin protocol on stdio: JSON Lines with the
//! envelope `{protocolVersion, requestId, payload}`. The device protocol is the
//! MicroCon BLE UART control service (UTF-8 JSON + LF frames): control commands
//! (`set_baud`, `get_status`) go to the command characteristic and responses
//! arrive as notifications on the status characteristic.
//!
//! Host behaviour baked in from debugging the Node version:
//! - the host silently drops `callHostApi` calls outside its allow-list, so
//!   every host call carries a timeout;
//! - `activate` must be answered immediately, BLE session loading happens in
//!   the background or the host reports "plugin runtime operation failed";
//! - `dispatchCommand` answers first and opens the tab afterwards.

use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::Duration;
use std::{process, thread};

const SERVICE_UUID: &str = "b7e70001-b5a3-f393-e0a9-e50e24dcca9e";
// The command characteristic receives framed JSON writes through the generic
// ble.write bridge.
const COMMAND_UUID: &str = "b7e70002-b5a3-f393-e0a9-e50e24dcca9e";
const STATUS_UUID: &str = "b7e70003-b5a3-f393-e0a9-e50e24dcca9e";

const BAUD_RATES: [i64; 12] = [
    1200, 2400, 4800, 9600, 14400, 19200, 38400, 57600, 115200, 230400, 460800, 921600,
];
// Protocol floor/ceiling documented by the device; the picker only offers
// rates inside this range but the apply path still re-validates.
const BAUD_MIN: i64 = 1200;
const BAUD_MAX: i64 = 921600;
// Fallback for devices whose status characteristic is not readable; the
// protocol document names 9600 as the shipping default.
const ASSUMED_CURRENT_BAUD: i64 = 9600;
const GATT_TIMEOUT_MS: u64 = 5000;
const RESPONSE_TIMEOUT_MS: u64 = 5000;
// The host silently drops calls outside its allow-list; the timeout keeps the
// UI from hanging forever on such calls.
const HOST_CALL_TIMEOUT_MS: u64 = 8000;
// Overlay telemetry cadence, migrated from the host's panel refresh loop:
// the plugin now owns the countdown and the periodic get_status poll.
const PANEL_REFRESH_SECS: u64 = 10;

const TAB_ID: &str = "microcon-helper";
const MANIFEST: &str = include_str!("../plugin.json");
const LOCALE_EN: &str = include_str!("../locales/en.json");

// Bundled translations keyed by the host locale tag (same tags as the host
// language packs). English is the fallback for unknown or missing locales.
const LOCALES: &[(&str, &str)] = &[
    ("en", LOCALE_EN),
    ("de", include_str!("../locales/de.json")),
    ("es-ES", include_str!("../locales/es-ES.json")),
    ("fr-FR", include_str!("../locales/fr-FR.json")),
    ("it", include_str!("../locales/it.json")),
    ("ja", include_str!("../locales/ja.json")),
    ("ko", include_str!("../locales/ko.json")),
    ("pt-BR", include_str!("../locales/pt-BR.json")),
    ("vi", include_str!("../locales/vi.json")),
    ("zh-CN", include_str!("../locales/zh-CN.json")),
    ("zh-TW", include_str!("../locales/zh-TW.json")),
];

fn plugin_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| {
        serde_json::from_str::<Value>(MANIFEST)
            .ok()
            .and_then(|manifest| {
                manifest
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .expect("plugin.json must contain a string id")
    })
}

fn locale_json(language: &str) -> Value {
    let source = LOCALES
        .iter()
        .find(|(tag, _)| *tag == language)
        .map_or(LOCALE_EN, |(_, source)| *source);
    serde_json::from_str(source).expect("bundled locale files must be valid JSON")
}

// Host BLE session entry; sessionId may arrive as a string or a number.
#[derive(Clone)]
struct Session {
    id: String,
    device_name: Option<String>,
    device_address: Option<String>,
    connected: bool,
}

impl Session {
    fn from_value(value: &Value) -> Option<Session> {
        let id = value.get("sessionId")?;
        if id.is_null() {
            return None;
        }
        Some(Session {
            id: value_to_string(id),
            device_name: optional_string(value.get("deviceName")),
            device_address: optional_string(value.get("deviceAddress")),
            connected: value
                .get("connected")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(text)) if !text.is_empty() => Some(text.clone()),
        _ => None,
    }
}

// Mirrors JS String(x): numbers become their decimal form, everything else
// falls back to the compact JSON form.
fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        Value::Bool(flag) => flag.to_string(),
        Value::Number(number) => number.to_string(),
        other => other.to_string(),
    }
}

// Renders f64 values the way JS String(number) would.
fn fmt_number(value: f64) -> String {
    if value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    } else {
        format!("{value}")
    }
}

fn hex_to_utf8(data: &str) -> Option<String> {
    if data.len() % 2 != 0 {
        return None;
    }
    let bytes: Option<Vec<u8>> = (0..data.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&data[index..index + 2], 16).ok())
        .collect();
    String::from_utf8(bytes?).ok()
}

// The status text is stored as data and rendered on demand so a language
// switch re-translates it, exactly like the Node renderStatus closures.
#[derive(Clone)]
enum StatusView {
    Key(&'static str),
    OutOfRange,
    NoSession,
    Applying {
        baud: i64,
        expected: i64,
        assumed: bool,
    },
    SentOk {
        from: i64,
        to: i64,
        // Actual rate reported by the device ack; a value differing from `to`
        // means the change did not stick (rollback to the previous rate).
        actual: Option<i64>,
    },
    Failed {
        message: String,
    },
}

struct State {
    messages: Value,
    sessions: Vec<Session>,
    selected_session_id: Option<String>,
    sessions_note: Option<String>,
    // Machine-readable telemetry rendered in the overlay card's status row;
    // Null parks the refresh countdown until the overlay reopens.
    panel_status: Value,
    // Session captured when the overlay opens; overlay refreshes reuse it so
    // focusing another terminal cannot silently move the binding.
    panel_session_id: Option<String>,
    // Overlay refresh ownership moved from the host to the plugin: the
    // ticker thread of the current generation runs the countdown and the
    // periodic get_status while the overlay is open (overlay_active), and
    // freezes them while the anchor pane lost focus (overlay_visible).
    // A newer open_overlay bumps the generation so an older loop exits.
    overlay_active: bool,
    overlay_visible: bool,
    overlay_generation: u64,
    panel_countdown: u8,
    // Nodes with a live ble.onNotification subscription (status
    // characteristic); released with offNotification on deactivation.
    notification_nodes: std::collections::HashSet<String>,
    selected_baud: String,
    busy: bool,
    status: StatusView,
}

impl State {
    fn new() -> State {
        State {
            messages: locale_json("en"),
            sessions: Vec::new(),
            selected_session_id: None,
            sessions_note: None,
            panel_status: Value::Null,
            panel_session_id: None,
            overlay_active: false,
            overlay_visible: false,
            overlay_generation: 0,
            panel_countdown: PANEL_REFRESH_SECS as u8,
            notification_nodes: Default::default(),
            selected_baud: "9600".to_string(),
            busy: false,
            status: StatusView::Key("initialStatus"),
        }
    }

    fn t(&self, key: &str, params: &[(&str, String)]) -> String {
        let mut text = self
            .messages
            .get(key)
            .and_then(Value::as_str)
            .unwrap_or(key)
            .to_string();
        for (name, value) in params {
            text = text.replace(&format!("{{{name}}}"), value);
        }
        text
    }
}

// Which frames may resolve a pending status-notification wait: restart waits
// are ack-id matched only, because periodic telemetry at the current baud
// would otherwise resolve the wait early and surface the raw frame to the
// user. Baud waits also accept a status frame already reporting the target
// rate; `Rate(None)` falls back to the picker's selected rate (tab apply).
#[derive(Clone, Copy)]
enum WaitMatch {
    AckIdOnly,
    Rate(Option<i64>),
}

struct Shared {
    state: Mutex<State>,
    // Pending callHostApi replies, matched by inner payload.requestId.
    pending_host: Mutex<HashMap<String, mpsc::Sender<Value>>>,
    // Pending status-notification wait; the generation counter makes the
    // timeout path only clear its own wait, never a newer one.
    pending_notify: Mutex<Option<(u64, i64, WaitMatch, mpsc::Sender<Value>)>>,
    notify_generation: AtomicU64,
    next_host_request: AtomicU64,
    next_command_id: AtomicU64,
    // Partial "/..." lines held back from the device, keyed by terminalId
    // (empty string for unknown terminals).
    line_buffers: Mutex<HashMap<String, LineBuffer>>,
    stdout_lock: Mutex<()>,
}

// Input-interceptor line buffer: bytes of the line held so far (no terminator).
#[derive(Default)]
struct LineBuffer {
    held: Vec<u8>,
    // Set after a consumed line terminated with CR: CRLF line endings leave
    // the pairing LF in the same or the next chunk, and forwarding it would
    // send a stray empty command to the device.
    swallow_lf: bool,
}

impl Shared {
    fn new() -> Arc<Shared> {
        Arc::new(Shared {
            state: Mutex::new(State::new()),
            pending_host: Mutex::new(HashMap::new()),
            pending_notify: Mutex::new(None),
            notify_generation: AtomicU64::new(0),
            next_host_request: AtomicU64::new(0),
            next_command_id: AtomicU64::new(0),
            line_buffers: Mutex::new(HashMap::new()),
            stdout_lock: Mutex::new(()),
        })
    }
}

fn write_frame(shared: &Shared, payload: &Value, request_id: Option<&str>) {
    let envelope = json!({
        "protocolVersion": 1,
        "requestId": request_id.map(Value::from).unwrap_or(Value::Null),
        "payload": payload,
    });
    let _guard = shared.stdout_lock.lock().unwrap_or_else(|e| e.into_inner());
    let stdout = io::stdout();
    let mut handle = stdout.lock();
    let _ = writeln!(handle, "{envelope}");
    let _ = handle.flush();
}

fn respond_ok(shared: &Shared, request_id: &str, value: Value) {
    write_frame(
        shared,
        &json!({
            "requestId": request_id,
            "result": { "status": "ok", "value": value },
        }),
        Some(request_id),
    );
}

fn respond_error(shared: &Shared, request_id: &str, code: &str, message: String) {
    write_frame(
        shared,
        &json!({
            "requestId": request_id,
            "result": {
                "status": "error",
                "error": { "code": code, "message": message, "recoverable": false },
            },
        }),
        Some(request_id),
    );
}

fn call_host(shared: &Shared, namespace: &str, method: &str, args: Value) -> Result<Value, String> {
    let request_id = format!(
        "host-{}",
        shared.next_host_request.fetch_add(1, Ordering::SeqCst) + 1
    );
    // Register the waiter before emitting the frame: the reader thread may see
    // the host reply the moment it is written, and an unregistered reply would
    // be misrouted as an unsupported request.
    let (tx, rx) = mpsc::channel();
    shared
        .pending_host
        .lock()
        .unwrap()
        .insert(request_id.clone(), tx);
    write_frame(
        shared,
        &json!({
            "type": "callHostApi",
            "requestId": request_id,
            "namespace": namespace,
            "method": method,
            "args": args,
        }),
        None,
    );
    match rx.recv_timeout(Duration::from_millis(HOST_CALL_TIMEOUT_MS)) {
        Ok(result) => {
            if result.get("status").and_then(Value::as_str) == Some("ok") {
                Ok(result.get("value").cloned().unwrap_or(Value::Null))
            } else {
                let message = result
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("host call failed")
                    .to_string();
                Err(message)
            }
        }
        Err(_) => {
            shared
                .pending_host
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&request_id);
            Err(format!("host call timeout: {namespace}.{method}"))
        }
    }
}

fn load_language(shared: &Shared) {
    // Older hosts may not expose the app namespace; stay with English.
    let locale = call_host(shared, "app", "getLocale", json!({}))
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "en".to_string());
    // locale_json falls back to English for tags without a bundled pack.
    let messages = locale_json(&locale);
    let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
    state.messages = messages;
}

fn set_status(shared: &Shared, status: StatusView) {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.status = status;
    }
    register_tab_view(shared);
}

// Active telemetry query: writes `get_status` to the command characteristic
// and resolves through the same status-notification slot as set_baud, so
// baud/battery/temperature always reflect the device's latest state. Returns
// None while a baud change owns the wait slot (keep showing the previous
// values); a failed query yields Null so the overlay row shows placeholders
// and the plugin's refresh countdown parks until the overlay reopens.
fn panel_status(shared: &Arc<Shared>) -> Option<Value> {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.busy {
            return None;
        }
        state.busy = true;
    }
    match panel_status_inner(shared) {
        Ok(status) => {
            shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
            Some(status)
        }
        Err(()) => {
            shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
            Some(Value::Null)
        }
    }
}

fn panel_status_inner(shared: &Arc<Shared>) -> Result<Value, ()> {
    let bound = shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .panel_session_id
        .clone();
    let session = pick_mc_session(shared, bound.as_deref()).map_err(|_| ())?;
    ensure_status_subscription(shared, &session).map_err(|_| ())?;
    let command_id = (shared.next_command_id.fetch_add(1, Ordering::SeqCst) + 1) as i64;
    let command = json!({ "proto": 1, "type": "get_status", "id": command_id });
    // Arm the wait before writing: the notification can race the worker.
    let (generation, notify_rx) = arm_status_wait(shared, command_id, WaitMatch::Rate(None));
    call_host(
        shared,
        "ble",
        "write",
        json!({
            "nodeId": session.id,
            "data": format!("{command}\n"),
            "dataEncoding": "utf8",
            "serviceUuid": SERVICE_UUID,
            "characteristicUuid": COMMAND_UUID,
            "writeType": "withResponse",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    )
    .map_err(|_| ())?;
    let response = conclude_status_wait(shared, generation, notify_rx).map_err(|_| ())?;
    Ok(panel_status_from_frame(&response))
}

// Maps a device status frame to the panel telemetry object the host renders
// in its bottom status row.
fn panel_status_from_frame(response: &Value) -> Value {
    json!({
        "baud": response.get("baud").and_then(Value::as_f64),
        "batteryPercent": response.get("batteryPercent").and_then(Value::as_f64),
        "temperatureC": if response.get("tempValid").and_then(Value::as_bool).unwrap_or(false) {
            response.get("tempC").and_then(Value::as_f64)
        } else {
            None
        },
    })
}

// Command responses (get_status/set_baud/restart acks) arrive as
// notifications on the status characteristic, so every command path needs
// this subscription guarantee; without it a fresh process writes the command
// but never sees the reply. Idempotent via notification_nodes bookkeeping.
fn ensure_status_subscription(shared: &Shared, session: &Session) -> Result<(), String> {
    let subscribed = shared
        .state
        .lock()
        .unwrap()
        .notification_nodes
        .contains(&session.id);
    if subscribed {
        return Ok(());
    }
    call_host(
        shared,
        "ble",
        "onNotification",
        json!({
            "nodeId": session.id,
            "characteristicUuid": STATUS_UUID,
            "serviceUuid": SERVICE_UUID,
        }),
    )?;
    shared
        .state
        .lock()
        .unwrap()
        .notification_nodes
        .insert(session.id.clone());
    Ok(())
}

fn render_status(state: &State) -> String {
    match &state.status {
        StatusView::Key(key) => state.t(key, &[]),
        StatusView::OutOfRange => state.t(
            "outOfRange",
            &[("min", BAUD_MIN.to_string()), ("max", BAUD_MAX.to_string())],
        ),
        StatusView::NoSession => state.t("noSession", &[]),
        StatusView::Applying {
            baud,
            expected,
            assumed,
        } => state.t(
            "applying",
            &[
                ("baud", fmt_number(*baud as f64)),
                ("expected", fmt_number(*expected as f64)),
                (
                    "assumed",
                    if *assumed {
                        state.t("assumedSuffix", &[])
                    } else {
                        String::new()
                    },
                ),
            ],
        ),
        StatusView::SentOk { from, to, actual } => match actual {
            Some(reverted) if *reverted != *to => state.t(
                "baudRolledBack",
                &[
                    ("from", fmt_number(*from as f64)),
                    ("to", fmt_number(*to as f64)),
                    ("actual", fmt_number(*reverted as f64)),
                ],
            ),
            _ => state.t(
                "baudApplied",
                &[
                    ("from", fmt_number(*from as f64)),
                    ("to", fmt_number(*to as f64)),
                ],
            ),
        },
        StatusView::Failed { message } => state.t("failed", &[("message", message.clone())]),
    }
}

// The overlay card's telemetry row. The host dropped its own status bar, so
// the plugin renders the readouts inside the schema pushed to the overlay
// tab; labels come from the active locale bundle and missing values use the
// same dash placeholder the host bar showed.
fn panel_status_row(state: &State) -> Value {
    let telemetry = if state.panel_status.is_object() {
        &state.panel_status
    } else {
        &Value::Null
    };
    let number = |name: &str| telemetry.get(name).and_then(Value::as_f64);
    // Icon + value readouts with the hint as a host-rendered tooltip,
    // mirroring the original host status bar: the icon carries meaning so
    // the row stays one line in every locale.
    let readout = |id: &str, icon: &str, hint_key: &str, value: Option<String>| {
        json!({
            "kind": "readout",
            "id": id,
            "icon": icon,
            "tooltip": state.t(hint_key, &[]),
            "text": value.unwrap_or_else(|| "–".to_string()),
        })
    };
    let mut children = vec![
        readout(
            "panelStatusBaud",
            "square-activity",
            "statusBaud",
            number("baud").map(fmt_number),
        ),
        readout(
            "panelStatusBattery",
            "battery-full",
            "statusBattery",
            number("batteryPercent").map(|v| format!("{v:.0}%")),
        ),
        readout(
            "panelStatusTemp",
            "thermometer",
            "statusTemp",
            number("temperatureC").map(|v| format!("{v:.1}°C")),
        ),
        json!({ "kind": "spacer", "id": "panelStatusSpacer" }),
    ];
    // The countdown renders only while telemetry is live, matching the host
    // bar this row replaces: a parked refresh has no next poll to announce.
    if state.panel_status.is_object() {
        children.push(json!({
            "kind": "readout",
            "id": "panelRefreshCountdown",
            "text": format!("{}s", state.panel_countdown),
        }));
    }
    json!({
        "kind": "row",
        "id": "panelStatusRow",
        "children": children,
    })
}

fn build_schema(state: &State) -> Value {
    let status_text = if state.sessions.is_empty() {
        match &state.sessions_note {
            Some(note) => note.clone(),
            None => render_status(state),
        }
    } else {
        render_status(state)
    };
    let mut controls = vec![
        json!({
            "kind": "row",
            "id": "baudRow",
            "children": [
                {
                    "kind": "select",
                    "id": "baud",
                    "label": state.t("baudLabel", &[]),
                    "value": state.selected_baud.clone(),
                    "options": BAUD_RATES
                        .iter()
                        .map(|rate| json!({ "label": rate.to_string(), "value": rate.to_string() }))
                        .collect::<Vec<Value>>(),
                },
                {
                    "kind": "button",
                    "id": "apply",
                    "label": state.t("apply", &[]),
                    "loading": state.busy,
                },
                // Pushes the restart button to the row's far edge.
                { "kind": "spacer" },
                {
                    "kind": "button",
                    "id": "restart",
                    "label": state.t("restartButton", &[]),
                    "loading": state.busy,
                },
            ],
        }),
        json!({
            "kind": "markdown",
            "id": "status",
            "text": status_text,
        }),
    ];
    if state.overlay_visible {
        controls.push(panel_status_row(state));
    }
    json!({
        "componentVersion": 1,
        "kind": "form",
        "title": state.t("title", &[]),
        "description": state.t("description", &[]),
        "sections": [
            {
                "id": "main",
                "controls": controls,
            },
        ],
    })
}

fn register_tab_view(shared: &Shared) {
    let schema = {
        let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        build_schema(&state)
    };
    write_frame(
        shared,
        &json!({
            "type": "registerContribution",
            "registration": {
                "registrationId": "microcon-helper-view",
                "pluginId": plugin_id(),
                "kind": "tab",
                "metadata": { "tabId": TAB_ID, "schema": schema },
            }
        }),
        None,
    );
}

fn register_view(shared: &Shared) {
    // ble.notification is excluded from the subscription whitelist: it arrives
    // directly from the onNotification bridge, so only i18n needs a registration.
    write_frame(
        shared,
        &json!({
            "type": "registerContribution",
            "registration": {
                "registrationId": "subscribe-i18n.languageChanged",
                "pluginId": plugin_id(),
                "kind": "event-subscription",
                "metadata": { "event": "i18n.languageChanged" },
            }
        }),
        None,
    );
    // Terminal input interceptor for the /mc debug channel; the host only
    // accepts this registration when the manifest declares terminalHooks.
    write_frame(
        shared,
        &json!({
            "type": "registerContribution",
            "registration": {
                "registrationId": "terminal-input-interceptor",
                "pluginId": plugin_id(),
                "kind": "terminal-input-interceptor",
                "metadata": { "command": "microconHelper.input" },
            }
        }),
        None,
    );
    register_tab_view(shared);
    write_frame(shared, &json!({ "type": "runtimeReady" }), None);
}

// Implicit target: focused terminal BLE session > first connected > first.
fn implicit_pick(shared: &Shared, sessions: &[Session]) -> Option<usize> {
    if sessions.is_empty() {
        return None;
    }
    if let Ok(metadata) = call_host(shared, "terminal", "getMetadata", json!({})) {
        let active = metadata.get("activeTarget");
        if let Some(active) = active {
            if active.get("terminalType").and_then(Value::as_str) == Some("ble") {
                if let Some(session_id) = active.get("sessionId") {
                    if !session_id.is_null() {
                        let wanted = value_to_string(session_id);
                        if let Some(index) = sessions.iter().position(|s| s.id == wanted) {
                            return Some(index);
                        }
                    }
                }
            }
        }
    }
    sessions.iter().position(|s| s.connected).or(Some(0))
}

fn load_sessions(shared: &Shared) {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sessions_note = None;
    }
    let loaded = call_host(shared, "ble", "sessions", json!({}));
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        match loaded {
            Ok(value) => {
                state.sessions = value
                    .get("sessions")
                    .and_then(Value::as_array)
                    .map(|entries| entries.iter().filter_map(Session::from_value).collect())
                    .unwrap_or_default();
                if state.sessions.is_empty() {
                    state.sessions_note = Some(state.t("noSessions", &[]));
                }
            }
            Err(message) => {
                state.sessions.clear();
                state.sessions_note = Some(state.t("failed", &[("message", message)]));
            }
        }
        // Fall back to the implicit target when the selection is stale.
        let selected_valid = state
            .selected_session_id
            .as_ref()
            .is_some_and(|selected| state.sessions.iter().any(|s| &s.id == selected));
        if state.selected_session_id.is_some() && !selected_valid {
            state.selected_session_id = None;
        }
    }
    let sessions = shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .sessions
        .clone();
    if shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .selected_session_id
        .is_none()
        && !sessions.is_empty()
    {
        if let Some(index) = implicit_pick(shared, &sessions) {
            shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .selected_session_id = Some(sessions[index].id.clone());
        }
    }
}

fn arm_status_wait(
    shared: &Shared,
    command_id: i64,
    wait: WaitMatch,
) -> (u64, mpsc::Receiver<Value>) {
    let (tx, rx) = mpsc::channel();
    let generation = shared.notify_generation.fetch_add(1, Ordering::SeqCst) + 1;
    *shared
        .pending_notify
        .lock()
        .unwrap_or_else(|e| e.into_inner()) = Some((generation, command_id, wait, tx));
    (generation, rx)
}

fn conclude_status_wait(
    shared: &Shared,
    generation: u64,
    rx: mpsc::Receiver<Value>,
) -> Result<Value, String> {
    match rx.recv_timeout(Duration::from_millis(RESPONSE_TIMEOUT_MS)) {
        Ok(message) => Ok(message),
        Err(_) => {
            // Only clear this wait, never a newer one.
            let mut pending = shared
                .pending_notify
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if matches!(&*pending, Some((registered, ..)) if *registered == generation) {
                *pending = None;
            }
            let message = shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .t("responseTimeout", &[]);
            Err(message)
        }
    }
}

fn handle_ble_notification(shared: &Shared, payload: &Value) {
    let (generation, command_id, wait, tx) = {
        let pending = shared
            .pending_notify
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        match pending.as_ref() {
            Some((generation, command_id, wait, tx)) => {
                (*generation, *command_id, *wait, tx.clone())
            }
            None => return,
        }
    };
    let Some(text) = payload
        .get("data")
        .and_then(Value::as_str)
        .and_then(hex_to_utf8)
    else {
        return;
    };
    for line in text.split('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(message) = serde_json::from_str::<Value>(trimmed) else {
            continue;
        };
        // Accept the device ack for this exact command id. Baud waits may also
        // resolve on a status frame already reporting the target rate (the /mc
        // path overrides the wanted rate; the tab path falls back to the
        // picker value). Periodic telemetry (old baud, no matching id) must
        // not resolve the wait, and restart waits ignore telemetry entirely.
        let id_matches = message.get("id").and_then(Value::as_i64) == Some(command_id);
        let baud_matches = match wait {
            WaitMatch::AckIdOnly => false,
            WaitMatch::Rate(wanted_baud) => {
                message.get("type").and_then(Value::as_str) == Some("status") && {
                    let wanted = wanted_baud.or_else(|| {
                        shared
                            .state
                            .lock()
                            .unwrap()
                            .selected_baud
                            .parse::<i64>()
                            .ok()
                    });
                    message
                        .get("baud")
                        .and_then(Value::as_i64)
                        .is_some_and(|rate| wanted.is_some_and(|target| rate == target))
                }
            }
        };
        if id_matches || baud_matches {
            let mut pending = shared
                .pending_notify
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if matches!(&*pending, Some((registered, ..)) if *registered == generation) {
                *pending = None;
            }
            let _ = tx.send(message);
            return;
        }
    }
}

fn apply_baud(shared: Arc<Shared>) {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.busy {
            return;
        }
        state.busy = true;
    }
    register_tab_view(&shared);
    let outcome = apply_baud_inner(&shared);
    if let Err(message) = outcome {
        set_status(&shared, StatusView::Failed { message });
    }
    shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    register_tab_view(&shared);
}

fn apply_baud_inner(shared: &Arc<Shared>) -> Result<(), String> {
    let baud: i64 = {
        let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        match state.selected_baud.parse::<i64>() {
            Ok(rate) if (BAUD_MIN..=BAUD_MAX).contains(&rate) => rate,
            _ => {
                set_status(shared, StatusView::OutOfRange);
                return Ok(());
            }
        }
    };

    // Target session: the explicit selection wins; when it is set but stale the
    // JS version reports "no session" instead of falling back to implicit.
    let explicit_selection = shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .selected_session_id
        .clone();
    let session = match &explicit_selection {
        Some(selected) => {
            let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            match state
                .sessions
                .iter()
                .find(|session| &session.id == selected)
            {
                Some(session) => session.clone(),
                None => {
                    drop(state);
                    set_status(shared, StatusView::NoSession);
                    return Ok(());
                }
            }
        }
        None => {
            let sessions = shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions
                .clone();
            match implicit_pick(shared, &sessions).map(|index| sessions[index].clone()) {
                Some(session) => session,
                None => {
                    set_status(shared, StatusView::NoSession);
                    return Ok(());
                }
            }
        }
    };
    let node_id = session.id.clone();

    ensure_status_subscription(shared, &session)?;

    // expectedBaud must carry the device's current rate; read the status
    // characteristic and fall back to the documented default when unreadable.
    let mut expected_baud = ASSUMED_CURRENT_BAUD;
    let mut expected_known = false;
    if let Ok(read) = call_host(
        shared,
        "ble",
        "read",
        json!({
            "nodeId": node_id,
            "characteristicUuid": STATUS_UUID,
            "serviceUuid": SERVICE_UUID,
            "encoding": "utf8",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    ) {
        let data = read.get("data").and_then(Value::as_str).unwrap_or("");
        if let Ok(device_status) = serde_json::from_str::<Value>(data.trim()) {
            if let Some(rate) = device_status.get("baud").and_then(Value::as_i64) {
                expected_baud = rate;
                expected_known = true;
            }
        }
    }

    set_status(
        shared,
        StatusView::Applying {
            baud,
            expected: expected_baud,
            assumed: !expected_known,
        },
    );

    let command_id = (shared.next_command_id.fetch_add(1, Ordering::SeqCst) + 1) as i64;
    let command = json!({
        "proto": 1,
        "type": "set_baud",
        "id": command_id,
        "baud": baud,
        "expectedBaud": expected_baud,
    });
    // Arm the status wait before writing: the notification can race the
    // worker between the write response and the wait registration.
    let (generation, notify_rx) = arm_status_wait(shared, command_id, WaitMatch::Rate(None));
    call_host(
        shared,
        "ble",
        "write",
        json!({
            "nodeId": node_id,
            "data": format!("{command}\n"),
            "dataEncoding": "utf8",
            "serviceUuid": SERVICE_UUID,
            "characteristicUuid": COMMAND_UUID,
            "writeType": "withResponse",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    )?;

    let response = conclude_status_wait(shared, generation, notify_rx)?;
    // The device ack doubles as fresh telemetry: publish it immediately so the
    // panel status row reflects the new baud (and battery/temperature) without
    // waiting for the next poll. Bare result acks carry no baud and keep the
    // previous telemetry.
    if response.get("baud").is_some() {
        shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .panel_status = panel_status_from_frame(&response);
    }
    let status = StatusView::SentOk {
        from: expected_baud,
        to: baud,
        actual: response.get("baud").and_then(Value::as_i64),
    };
    set_status(shared, status);
    Ok(())
}

fn restart_device(shared: Arc<Shared>) {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.busy {
            return;
        }
        state.busy = true;
    }
    register_tab_view(&shared);
    let outcome = restart_inner(&shared);
    if let Err(message) = outcome {
        set_status(&shared, StatusView::Failed { message });
    }
    shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    register_tab_view(&shared);
}

fn restart_inner(shared: &Arc<Shared>) -> Result<(), String> {
    // The restart button also serves the overlay, so prefer the overlay's
    // bound session and fall back to implicit resolution after a reconnect.
    let bound = shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .panel_session_id
        .clone();
    let session = pick_mc_session(shared, bound.as_deref())?;
    reboot_device(shared, &session)?;
    // The device drops the BLE link right after acking; clearing the telemetry
    // parks the overlay refresh countdown until the overlay reopens.
    shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .panel_status = Value::Null;
    set_status(shared, StatusView::Key("restartDone"));
    Ok(())
}

// Device round trip shared by the panel restart button and /mc reboot:
// status subscription, restart command write, then the ack wait. The device
// reboots, not the session: the BLE link drops as a side effect and the user
// reconnects afterwards.
fn reboot_device(shared: &Shared, session: &Session) -> Result<(), String> {
    ensure_status_subscription(shared, session)?;
    let command_id = (shared.next_command_id.fetch_add(1, Ordering::SeqCst) + 1) as i64;
    let command = json!({
        "proto": 1,
        "type": "restart",
        "id": command_id,
        "confirm": "restart",
    });
    // Ack-id matching only: a restart wait must never resolve on periodic
    // telemetry, or the raw frame would be shown to the user as the outcome.
    let (generation, notify_rx) = arm_status_wait(shared, command_id, WaitMatch::AckIdOnly);
    call_host(
        shared,
        "ble",
        "write",
        json!({
            "nodeId": session.id,
            "data": format!("{command}\n"),
            "dataEncoding": "utf8",
            "serviceUuid": SERVICE_UUID,
            "characteristicUuid": COMMAND_UUID,
            "writeType": "withResponse",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    )?;
    let response = conclude_status_wait(shared, generation, notify_rx)?;
    if response.get("ok").and_then(Value::as_bool) != Some(true) {
        return Err(response.to_string());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// /mc command channel (terminal input interceptor)
//
// The interceptor holds back any line that could still become "/mc ..." so the
// device never sees the debug command. Held bytes are echoed locally through
// the hook's screen-only channel ("echo" + "suppress"), so the user sees what
// they type. Lines that turn out unrelated erase the local echo and are
// replayed verbatim to the device. Executed commands run on a background
// thread (the hook budget is 5ms) and print through terminal.writeLocal, the
// display-only channel that never reaches the device or the session log.
// ---------------------------------------------------------------------------

const MC_COMMAND: &str = "/mc";
const ERASE_SEQUENCE: &[u8] = b"\x08 \x08";

// The terminal screen is a raw VT stream: a bare LF only moves down one row
// without returning the cursor, so every newline in locally displayed text is
// normalized to CRLF (existing CR and CRLF sequences are preserved).
fn crlf(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 8);
    let mut chars = text.chars().peekable();
    while let Some(char) = chars.next() {
        match char {
            '\r' => {
                out.push('\r');
                if chars.peek() == Some(&'\n') {
                    chars.next();
                    out.push('\n');
                }
            }
            '\n' => out.push_str("\r\n"),
            other => out.push(other),
        }
    }
    out
}

fn push_erase(echo: &mut Vec<u8>, count: usize) {
    for _ in 0..count {
        echo.extend_from_slice(ERASE_SEQUENCE);
    }
}

// Feeds one interceptor chunk through the per-terminal line buffer and returns
// the hook response: {"echo", "suppress"} to hold with local display,
// {"echo", "text"} to erase the echo and replay, {"text"} for plain
// passthrough, or null when nothing needs to happen.
fn feed_input(shared: &Arc<Shared>, terminal_id: Option<&str>, data: &str) -> Value {
    let key = terminal_id.unwrap_or("").to_string();
    let mut out: Vec<u8> = Vec::new();
    let mut echo: Vec<u8> = Vec::new();
    let mut executed_lines: Vec<String> = Vec::new();
    {
        let mut buffers = shared
            .line_buffers
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let buffer = buffers.entry(key).or_default();
        for &byte in data.as_bytes() {
            if buffer.swallow_lf {
                buffer.swallow_lf = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if buffer.held.is_empty() {
                if byte == b'/' {
                    buffer.held.push(byte);
                    echo.push(byte);
                } else {
                    out.push(byte);
                }
                continue;
            }
            match byte {
                b'\r' | b'\n' => {
                    let line = String::from_utf8_lossy(&buffer.held).to_string();
                    if line == MC_COMMAND || line.starts_with(&format!("{MC_COMMAND} ")) {
                        // Consumed: close the locally echoed command line and
                        // execute it; results arrive via terminal.writeLocal.
                        echo.extend_from_slice(b"\r\n");
                        executed_lines.push(line);
                        // Drop the pairing LF when Enter sends CRLF.
                        buffer.swallow_lf = byte == b'\r';
                    } else {
                        // Unrelated line: erase the local echo, then replay the
                        // whole line verbatim so the device re-echoes it.
                        push_erase(&mut echo, buffer.held.len());
                        out.extend_from_slice(&buffer.held);
                        out.push(byte);
                    }
                    buffer.held.clear();
                }
                0x7f | 0x08 => {
                    // The held bytes were echoed locally, so backspace erases
                    // them on screen before popping.
                    if buffer.held.pop().is_some() {
                        echo.extend_from_slice(ERASE_SEQUENCE);
                    }
                }
                // Control bytes (Ctrl-C, ESC, Ctrl-U, ...) end the hold
                // immediately: they are line-editing or abort signals that must
                // reach the device without waiting for Enter, unlike a plain
                // divergence which used to release early and broke corrections
                // (after a release the retyped line went to the device and the
                // plugin never saw the final Enter).
                byte if byte < 0x20 => {
                    push_erase(&mut echo, buffer.held.len());
                    out.extend_from_slice(&buffer.held);
                    out.push(byte);
                    buffer.held.clear();
                }
                // Everything else keeps holding until the terminator, so any
                // amount of typing, deleting, and retyping still lands in the
                // plugin when the final line is a /mc command.
                _ => {
                    buffer.held.push(byte);
                    echo.push(byte);
                }
            }
        }
    }
    for line in executed_lines {
        execute_mc_command(shared, terminal_id.map(str::to_string), line);
    }
    match (!echo.is_empty(), !out.is_empty()) {
        (false, false) => json!(null),
        (true, false) => json!({
            "echo": String::from_utf8_lossy(&echo).to_string(),
            "suppress": true,
        }),
        (false, true) => json!({ "text": String::from_utf8_lossy(&out).to_string() }),
        (true, true) => json!({
            "echo": String::from_utf8_lossy(&echo).to_string(),
            "text": String::from_utf8_lossy(&out).to_string(),
        }),
    }
}

fn execute_mc_command(shared: &Arc<Shared>, terminal_id: Option<String>, line: String) {
    let shared = Arc::clone(shared);
    thread::spawn(move || {
        let outcome = run_mc_command(&shared, terminal_id.as_deref(), &line);
        let text = match outcome {
            Ok(text) => text,
            Err(message) => message,
        };
        // The command line was already echoed locally while typing and closed
        // by the hook's terminator echo; only the result text goes to the
        // display channel, so the reply never repeats the command line.
        let payload = crlf(&format!("{text}\n"));
        // Target the terminal the command was typed in: without terminalId the
        // host falls back to the active pane, which may have moved on while the
        // command ran.
        let mut args = json!({ "text": payload });
        if let Some(terminal_id) = &terminal_id {
            args["terminalId"] = json!(terminal_id);
        }
        let _ = call_host_retry(&shared, "terminal", "writeLocal", args);
    });
}

fn run_mc_command(
    shared: &Arc<Shared>,
    terminal_id: Option<&str>,
    line: &str,
) -> Result<String, String> {
    let args: Vec<&str> = line.split_whitespace().collect();
    // First-letter abbreviations resolve like the spelled-out commands; the
    // help footer advertises them.
    match args.get(1).copied() {
        None | Some("help") | Some("h") => {
            // The help header reuses the per-language plugin title
            // ("MicroCon 助手" / "MicroCon Helper") instead of a second copy.
            let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            let title = state.t("title", &[]);
            Ok(state.t("mcHelp", &[("title", title)]))
        }
        Some("status") | Some("s") => mc_status(shared, terminal_id),
        Some("baud") | Some("b") => mc_baud(shared, terminal_id, args.get(2).copied()),
        Some("reboot") | Some("r") => mc_reboot(shared, terminal_id),
        Some(other) => {
            let message = shared
                .state
                .lock()
                .unwrap()
                .t("mcUnknown", &[("command", other.to_string())]);
            Err(message)
        }
    }
}

// Host APIs bounce through a stub resolver while a terminal hook is running;
// the millisecond-wide window yields this error, so retry once.
fn call_host_retry(
    shared: &Shared,
    namespace: &str,
    method: &str,
    args: Value,
) -> Result<Value, String> {
    match call_host(shared, namespace, method, args.clone()) {
        Err(message) if message.contains("terminal input hook") => {
            thread::sleep(Duration::from_millis(20));
            call_host(shared, namespace, method, args)
        }
        other => other,
    }
}

// /mc targets the terminal the command was typed in; fall back to the implicit
// pick (focused BLE terminal > first connected > first).
fn pick_mc_session(shared: &Shared, terminal_id: Option<&str>) -> Result<Session, String> {
    let loaded = call_host_retry(shared, "ble", "sessions", json!({}))?;
    let sessions: Vec<Session> = loaded
        .get("sessions")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().filter_map(Session::from_value).collect())
        .unwrap_or_default();
    if let Some(terminal_id) = terminal_id.filter(|id| !id.is_empty()) {
        if let Some(session) = sessions.iter().find(|session| session.id == terminal_id) {
            return Ok(session.clone());
        }
    }
    implicit_pick(shared, &sessions)
        .map(|index| sessions[index].clone())
        .ok_or_else(|| {
            shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .t("noSession", &[])
        })
}

fn mc_status(shared: &Shared, terminal_id: Option<&str>) -> Result<String, String> {
    let session = pick_mc_session(shared, terminal_id)?;
    let read = call_host_retry(
        shared,
        "ble",
        "read",
        json!({
            "nodeId": session.id,
            "characteristicUuid": STATUS_UUID,
            "serviceUuid": SERVICE_UUID,
            "encoding": "utf8",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    )?;
    let data = read.get("data").and_then(Value::as_str).unwrap_or("");
    let status: Value = serde_json::from_str(data.trim()).map_err(|_| {
        shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .t("mcStatusBadFrame", &[])
    })?;
    let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
    // Field order matches the /mc status contract: baud, battery, temperature.
    let mut parts: Vec<String> = Vec::new();
    if let Some(value) = status.get("baud").and_then(Value::as_f64) {
        parts.push(state.t("detailCurrent", &[("value", fmt_number(value))]));
    }
    if let Some(value) = status.get("batteryPercent").and_then(Value::as_f64) {
        parts.push(state.t("detailBattery", &[("value", fmt_number(value))]));
    }
    if status
        .get("tempValid")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        && let Some(value) = status.get("tempC").and_then(Value::as_f64)
    {
        parts.push(state.t("detailTemp", &[("value", fmt_number(value))]));
    }
    if parts.is_empty() {
        Ok(status.to_string())
    } else {
        Ok(parts.join(&state.t("detailJoiner", &[])))
    }
}

fn mc_baud(
    shared: &Arc<Shared>,
    terminal_id: Option<&str>,
    rate: Option<&str>,
) -> Result<String, String> {
    {
        // Share the tab's busy flag: a /mc set_baud and an Apply click must
        // never interleave their status-notification waits (single slot).
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.busy {
            return Err(state.t("busy", &[]));
        }
        state.busy = true;
    }
    let result = mc_baud_inner(shared, terminal_id, rate);
    shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    result
}

fn mc_baud_inner(
    shared: &Arc<Shared>,
    terminal_id: Option<&str>,
    rate: Option<&str>,
) -> Result<String, String> {
    let baud: i64 = {
        let state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        match rate.map(str::parse::<i64>) {
            Some(Ok(value)) if (BAUD_MIN..=BAUD_MAX).contains(&value) => value,
            // A numeric rate outside the range gets the bounded message; a
            // missing or non-numeric argument gets the usage line.
            Some(Ok(_)) => {
                return Err(state.t(
                    "outOfRange",
                    &[("min", BAUD_MIN.to_string()), ("max", BAUD_MAX.to_string())],
                ));
            }
            None | Some(Err(_)) => return Err(state.t("mcBaudUsage", &[])),
        }
    };
    let session = pick_mc_session(shared, terminal_id)?;
    let where_ = session
        .device_name
        .clone()
        .or_else(|| session.device_address.clone())
        .unwrap_or_else(|| session.id.clone());
    // Subscribe through the shared helper so the node is recorded for
    // offNotification on deactivate; a direct call would re-subscribe on
    // every command and leak the subscription past deactivate.
    ensure_status_subscription(shared, &session)?;
    // expectedBaud must carry the device's current rate; fall back to the
    // documented default when the status characteristic is unreadable.
    let mut expected_baud = ASSUMED_CURRENT_BAUD;
    if let Ok(read) = call_host_retry(
        shared,
        "ble",
        "read",
        json!({
            "nodeId": session.id,
            "characteristicUuid": STATUS_UUID,
            "serviceUuid": SERVICE_UUID,
            "encoding": "utf8",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    ) {
        let data = read.get("data").and_then(Value::as_str).unwrap_or("");
        if let Ok(device_status) = serde_json::from_str::<Value>(data.trim()) {
            if let Some(value) = device_status.get("baud").and_then(Value::as_i64) {
                expected_baud = value;
            }
        }
    }
    let command_id = (shared.next_command_id.fetch_add(1, Ordering::SeqCst) + 1) as i64;
    let command = json!({
        "proto": 1,
        "type": "set_baud",
        "id": command_id,
        "baud": baud,
        "expectedBaud": expected_baud,
    });
    // Arm the wait before writing so the ack cannot race the registration.
    let (generation, notify_rx) = arm_status_wait(shared, command_id, WaitMatch::Rate(Some(baud)));
    call_host_retry(
        shared,
        "ble",
        "write",
        json!({
            "nodeId": session.id,
            "data": format!("{command}\n"),
            "dataEncoding": "utf8",
            "serviceUuid": SERVICE_UUID,
            "characteristicUuid": COMMAND_UUID,
            "writeType": "withResponse",
            "timeoutMs": GATT_TIMEOUT_MS,
        }),
    )?;
    let response = conclude_status_wait(shared, generation, notify_rx)?;
    // Same immediate telemetry publish as the tab's apply path: the /mc baud
    // change must also refresh the overlay row without waiting for a poll.
    // The ack may be a bare result without telemetry; keep the old values then.
    if response.get("baud").is_some() {
        shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .panel_status = panel_status_from_frame(&response);
    }
    let message = shared.state.lock().unwrap_or_else(|e| e.into_inner()).t(
        "mcBaudOk",
        &[("baud", fmt_number(baud as f64)), ("where", where_)],
    );
    Ok(message)
}

// /mc reboot shares the busy flag and the status-notification wait slot with
// the tab paths, and reuses the panel restart message ("please reconnect").
fn mc_reboot(shared: &Arc<Shared>, terminal_id: Option<&str>) -> Result<String, String> {
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.busy {
            return Err(state.t("busy", &[]));
        }
        state.busy = true;
    }
    let result = mc_reboot_inner(shared, terminal_id);
    shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy = false;
    result
}

fn mc_reboot_inner(shared: &Arc<Shared>, terminal_id: Option<&str>) -> Result<String, String> {
    let session = pick_mc_session(shared, terminal_id)?;
    reboot_device(shared, &session)?;
    // Same telemetry cleanup as the button path: the BLE link is gone after
    // the ack, so the overlay refresh countdown parks.
    shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .panel_status = Value::Null;
    let message = shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .t("restartDone", &[]);
    Ok(message)
}

/// Chip states in manifest declaration order. The host stores and renders
/// whatever state id this reports back; it never interprets the ids itself.
const TOOLBAR_ACTION_ID: &str = "mc-helper-mode";
const TOOLBAR_OVERLAY_STATE: &str = "overlay";
const TOOLBAR_STATE_ORDER: [&str; 3] = ["off", "inline", TOOLBAR_OVERLAY_STATE];

fn report_next_toolbar_state(shared: &Shared, payload: &Value) {
    if payload.get("actionId").and_then(Value::as_str) != Some(TOOLBAR_ACTION_ID) {
        return;
    }
    let Some(pane_id) = payload.get("paneId").and_then(Value::as_u64) else {
        return;
    };
    let current = payload
        .get("currentState")
        .and_then(Value::as_str)
        .unwrap_or(TOOLBAR_STATE_ORDER[0]);
    let index = TOOLBAR_STATE_ORDER
        .iter()
        .position(|state| *state == current)
        .unwrap_or(0);
    let next = TOOLBAR_STATE_ORDER[(index + 1) % TOOLBAR_STATE_ORDER.len()];
    // The host renders the overlay card as soon as it accepts this reported
    // state, before the overlayOpened event reaches open_overlay. Register
    // the schema with the status row first (placeholder values until
    // telemetry arrives) so the card keeps its final height on first paint.
    if next == TOOLBAR_OVERLAY_STATE {
        shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .overlay_visible = true;
        register_tab_view(shared);
    }
    let _ = call_host(
        shared,
        "toolbar",
        "reportActionState",
        json!({
            "actionId": TOOLBAR_ACTION_ID,
            "paneId": pane_id,
            "stateId": next,
        }),
    );
}

// Overlay lifecycle: the host only signals open/close/visibility events; the
// refresh countdown and the periodic get_status poll belong to this plugin.
// The opening pane's session is captured once (the toolbar chip belongs to
// that pane, so it holds focus right now) and a fresh get_status runs
// immediately: a parked panel (Null telemetry freezes the countdown) can
// only un-park through this open-time poll or a later successful tab apply.
fn open_overlay(shared: &Arc<Shared>, payload: &Value) {
    if payload.get("actionId").and_then(Value::as_str) != Some(TOOLBAR_ACTION_ID) {
        return;
    }
    let session = pick_mc_session(shared, None).ok().map(|session| session.id);
    let generation = {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.panel_session_id = session;
        state.overlay_active = true;
        // A freshly opened overlay is shown; the host follows up with
        // visibility changes only when they actually happen.
        state.overlay_visible = true;
        state.panel_countdown = PANEL_REFRESH_SECS as u8;
        state.overlay_generation += 1;
        state.overlay_generation
    };
    if let Some(status) = panel_status(shared) {
        shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .panel_status = status;
    }
    register_tab_view(shared);
    run_panel_ticker(Arc::clone(shared), generation);
}

fn close_overlay(shared: &Arc<Shared>, payload: &Value) {
    if payload.get("actionId").and_then(Value::as_str) != Some(TOOLBAR_ACTION_ID) {
        return;
    }
    {
        let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.overlay_active = false;
        state.overlay_visible = false;
        state.panel_session_id = None;
    }
    // The ticker observes overlay_active at its next 1s tick and exits; the
    // row must already be gone from the pushed schema.
    register_tab_view(shared);
}

fn set_overlay_visibility(shared: &Arc<Shared>, payload: &Value) {
    if payload.get("actionId").and_then(Value::as_str) != Some(TOOLBAR_ACTION_ID) {
        return;
    }
    let Some(visible) = payload.get("visible").and_then(Value::as_bool) else {
        return;
    };
    shared
        .state
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .overlay_visible = visible;
}

// Single-instance guard: exactly one loop runs per open_overlay generation.
// A superseded or closed overlay breaks at the next tick, so older loops
// retire without ever polling the device again. All BLE work happens here,
// never on the protocol reader thread.
fn run_panel_ticker(shared: Arc<Shared>, generation: u64) {
    loop {
        thread::sleep(Duration::from_secs(1));
        let fire = {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.overlay_active || state.overlay_generation != generation {
                break;
            }
            // Frozen while the anchor pane lost focus; parked while the last
            // get_status failed — the countdown only runs on live telemetry.
            if state.overlay_visible && state.panel_status.is_object() {
                state.panel_countdown -= 1;
            }
            if state.panel_countdown == 0 {
                state.panel_countdown = PANEL_REFRESH_SECS as u8;
                true
            } else {
                false
            }
        };
        if fire {
            // None means a tab operation owns the wait slot: keep showing the
            // previous telemetry instead of failing the refresh.
            if let Some(status) = panel_status(&shared) {
                shared
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .panel_status = status;
            }
        }
        register_tab_view(&shared);
    }
}

fn handle_ui_event(shared: &Arc<Shared>, payload: &Value) {
    let control_id = payload
        .get("controlId")
        .and_then(Value::as_str)
        .unwrap_or("");
    let event_type = payload.get("type").and_then(Value::as_str).unwrap_or("");
    match (control_id, event_type) {
        ("baud", "change") => {
            let value = match payload.get("value") {
                Some(value) => value_to_string(value),
                None => return,
            };
            shared
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .selected_baud = value;
        }
        ("apply", "click") => {
            let shared = Arc::clone(shared);
            thread::spawn(move || apply_baud(shared));
        }
        ("restart", "click") => {
            let shared = Arc::clone(shared);
            thread::spawn(move || restart_device(shared));
        }
        _ => {}
    }
}

fn handle_request(shared: Arc<Shared>, payload: Value) {
    let Some(request_id) = payload
        .get("requestId")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    let kind_type = payload
        .pointer("/kind/type")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    match kind_type.as_str() {
        "activate" => {
            load_language(&shared);
            register_view(&shared);
            // Answer the activation immediately; the session list loads in the
            // background because the host may silently drop ble calls and must
            // never see the activation response wait for them.
            respond_ok(&shared, &request_id, json!({ "activated": true }));
            thread::spawn(move || {
                load_sessions(&shared);
                if !shared.state.lock().unwrap_or_else(|e| e.into_inner()).busy {
                    register_tab_view(&shared);
                }
            });
        }
        "sendEvent" => {
            let event = payload.pointer("/kind/event").cloned().unwrap_or(json!({}));
            let name = event.get("name").and_then(Value::as_str).unwrap_or("");
            match name {
                "ui.event" => {
                    handle_ui_event(&shared, event.get("payload").unwrap_or(&Value::Null))
                }
                "ble.notification" => {
                    handle_ble_notification(&shared, event.get("payload").unwrap_or(&Value::Null))
                }
                "i18n.languageChanged" => {
                    // The request worker also serves terminal input hooks on a
                    // 5ms budget, so a host call that can stall until timeout
                    // must not run here; the refresh keeps the load-then-
                    // republish order on its own thread.
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || {
                        load_language(&shared);
                        register_tab_view(&shared);
                    });
                }
                "toolbar.actionClicked" => {
                    // The host delivers chip clicks as opaque events and the
                    // plugin owns the state semantics. The report call must
                    // not block the reader thread that will deliver the
                    // host's response.
                    let click = event.get("payload").cloned().unwrap_or(json!({}));
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || report_next_toolbar_state(&shared, &click));
                }
                "toolbar.overlayOpened" => {
                    // Binding and the first telemetry poll hit the device, so
                    // they run on the overlay's background thread; this also
                    // becomes the ticker loop for this overlay generation.
                    let info = event.get("payload").cloned().unwrap_or(json!({}));
                    let shared = Arc::clone(&shared);
                    thread::spawn(move || open_overlay(&shared, &info));
                }
                "toolbar.overlayClosed" => {
                    let info = event.get("payload").cloned().unwrap_or(json!({}));
                    close_overlay(&shared, &info);
                }
                "toolbar.overlayVisibilityChanged" => {
                    let info = event.get("payload").cloned().unwrap_or(json!({}));
                    set_overlay_visibility(&shared, &info);
                }
                _ => {}
            }
            respond_ok(&shared, &request_id, json!({ "received": name }));
        }
        "dispatchCommand" => {
            let command = payload
                .pointer("/kind/command")
                .and_then(Value::as_str)
                .unwrap_or("");
            if command == "microconHelper.input" {
                // Terminal input hook: must answer inside the 5ms budget, so
                // only the synchronous line buffer runs here. Matched /mc
                // commands are executed on a background thread.
                let data = payload
                    .pointer("/kind/args/data")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let terminal_id = payload
                    .pointer("/kind/args/terminalId")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                let value = feed_input(&shared, terminal_id.as_deref(), &data);
                respond_ok(&shared, &request_id, value);
            } else {
                respond_error(
                    &shared,
                    &request_id,
                    "unsupported_command",
                    format!("Unsupported command {command}"),
                );
            }
        }
        "health" => respond_ok(&shared, &request_id, json!({ "ok": true })),
        "deactivate" | "kill" => {
            // Release status-characteristic subscriptions before acking, so
            // the host can tear down CCCD state while this process is still
            // alive to answer. offNotification is idempotent per contract.
            let nodes: Vec<String> = {
                let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
                std::mem::take(&mut state.notification_nodes)
                    .into_iter()
                    .collect()
            };
            for node_id in nodes {
                let _ = call_host(
                    &shared,
                    "ble",
                    "offNotification",
                    json!({
                        "nodeId": node_id,
                        "characteristicUuid": STATUS_UUID,
                        "serviceUuid": SERVICE_UUID,
                    }),
                );
            }
            respond_ok(&shared, &request_id, json!({ "stopped": true }));
            process::exit(0);
        }
        other => respond_error(
            &shared,
            &request_id,
            "unsupported_request",
            format!("Unsupported request {other}"),
        ),
    }
}

fn main() {
    let shared = Shared::new();
    // Requests are handled on a dedicated worker so the main thread can keep
    // reading stdin while a handler blocks on a host call (activate awaits
    // getLocale before answering; blocking the reader would deadlock).
    let (requests_tx, requests_rx) = mpsc::channel::<Value>();
    let worker = Arc::clone(&shared);
    thread::spawn(move || {
        while let Ok(payload) = requests_rx.recv() {
            handle_request(Arc::clone(&worker), payload);
        }
    });
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Ok(envelope) = serde_json::from_str::<Value>(trimmed) else {
            eprintln!("Plugin received an invalid protocol frame.");
            continue;
        };
        let Some(payload) = envelope.get("payload") else {
            continue;
        };
        // Host replies are matched by the inner payload.requestId; the outbound
        // envelope always carries requestId null.
        if let Some(reply_id) = payload
            .get("requestId")
            .and_then(Value::as_str)
            .map(str::to_string)
        {
            let sender = shared
                .pending_host
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&reply_id);
            if let Some(sender) = sender {
                let _ = sender.send(payload.get("result").cloned().unwrap_or(Value::Null));
                continue;
            }
        }
        let _ = requests_tx.send(payload.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_hex_notification_data() {
        assert_eq!(
            hex_to_utf8("7b2274797065223a22737461747573227d").as_deref(),
            Some("{\"type\":\"status\"}")
        );
        assert_eq!(hex_to_utf8("").as_deref(), Some(""));
        assert_eq!(hex_to_utf8("zz"), None);
        assert_eq!(hex_to_utf8("7"), None);
    }

    // Every bundled pack must cover the English key set: a missing key would
    // render as a lookup failure for that language's users, and placeholder
    // mismatches would break message formatting at runtime.
    #[test]
    fn bundled_locales_match_the_english_key_set_and_placeholders() {
        let english = locale_json("en");
        let keys = |locale: &Value| {
            locale
                .as_object()
                .expect("locale files must be JSON objects")
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        };
        let placeholders = |value: &Value| {
            let text = value.as_str().expect("locale values must be strings");
            let mut found = Vec::new();
            let mut rest = text;
            while let Some(start) = rest.find('{') {
                let end = rest[start..]
                    .find('}')
                    .expect("locale placeholders must be closed");
                found.push(rest[start + 1..start + end].to_string());
                rest = &rest[start + end + 1..];
            }
            found
        };
        let english_keys = keys(&english);

        for (tag, source) in LOCALES.iter().skip(1) {
            let locale = serde_json::from_str::<Value>(source)
                .unwrap_or_else(|error| panic!("locale {tag} must be valid JSON: {error}"));
            assert_eq!(keys(&locale), english_keys, "key set mismatch for {tag}");
            for (key, english_value) in english.as_object().unwrap() {
                let value = &locale[key];
                assert_eq!(
                    placeholders(value),
                    placeholders(english_value),
                    "placeholder mismatch for {tag}.{key}"
                );
            }
        }
    }

    #[test]
    fn locale_lookup_falls_back_to_english() {
        assert_eq!(locale_json("zh-CN")["title"], "MicroCon 助手");
        assert_eq!(locale_json("de")["baudLabel"], "Baudrate");
        assert_eq!(
            locale_json("not-a-language")["mcBaudUsage"],
            "Usage: /mc baud <rate> (1200–921600)"
        );
    }

    #[test]
    fn formats_numbers_like_the_device_frames() {
        assert_eq!(fmt_number(115200.0), "115200");
        assert_eq!(fmt_number(36.5), "36.5");
        assert_eq!(fmt_number(0.0), "0");
    }

    // The overlay card renders whatever schema the plugin pushes, so the
    // status row contract (formatting, dash placeholders, visibility gating
    // and the dropped panelStatus field) is asserted on the built schema.
    #[test]
    fn overlay_status_row_formats_values_and_follows_visibility() {
        let mut state = State::new();
        state.overlay_visible = true;
        state.panel_status = json!({
            "baud": 115200.0,
            "batteryPercent": 88.0,
            "temperatureC": Value::Null,
        });
        state.panel_countdown = 7;
        let schema = build_schema(&state);
        let controls = schema["sections"][0]["controls"].as_array().unwrap();
        assert_eq!(controls.len(), 3, "status row appended while visible");
        let row = &controls[2];
        assert_eq!(row["kind"], json!("row"));
        assert_eq!(row["id"], json!("panelStatusRow"));
        assert_eq!(row["children"][0]["kind"], json!("readout"));
        assert_eq!(row["children"][0]["icon"], json!("square-activity"));
        assert_eq!(row["children"][0]["tooltip"], json!("Current baud rate"));
        assert_eq!(row["children"][0]["text"], json!("115200"));
        assert_eq!(row["children"][1]["icon"], json!("battery-full"));
        assert_eq!(row["children"][1]["tooltip"], json!("Battery level"));
        assert_eq!(row["children"][1]["text"], json!("88%"));
        assert_eq!(
            row["children"][2]["text"],
            json!("–"),
            "missing temperature renders the dash placeholder"
        );
        assert_eq!(row["children"][3]["kind"], json!("spacer"));
        assert_eq!(row["children"][4]["text"], json!("7s"));

        state.overlay_visible = false;
        let schema = build_schema(&state);
        assert_eq!(
            schema["sections"][0]["controls"].as_array().unwrap().len(),
            2,
            "no status row while the overlay is hidden"
        );
        assert!(schema.get("panelStatus").is_none());
    }

    #[test]
    fn parses_sessions_from_the_host_value() {
        let value = json!([
            { "sessionId": "s1", "deviceName": "MicroCon", "connected": true },
            { "sessionId": 42, "deviceAddress": "AA:BB", "connected": false },
            { "connected": false }
        ]);
        let sessions: Vec<Session> = value
            .as_array()
            .unwrap()
            .iter()
            .filter_map(Session::from_value)
            .collect();
        assert_eq!(sessions.len(), 2);
        assert_eq!(sessions[0].id, "s1");
        assert!(sessions[0].connected);
        assert_eq!(sessions[1].id, "42");
        assert!(!sessions[1].connected);
    }

    #[test]
    fn input_interceptor_holds_divergence_until_terminator() {
        let shared = Shared::new();
        // Plain input passes through untouched.
        assert_eq!(
            feed_input(&shared, Some("t1"), "hello\r"),
            json!({ "text": "hello\r" })
        );
        // A held partial "/mc" prefix echoes locally and is suppressed.
        assert_eq!(
            feed_input(&shared, Some("t1"), "/m"),
            json!({ "echo": "/m", "suppress": true })
        );
        // Divergence keeps holding with local echo; the unrelated line is only
        // erased and replayed when its terminator arrives.
        assert_eq!(
            feed_input(&shared, Some("t1"), "x"),
            json!({ "echo": "x", "suppress": true })
        );
        assert_eq!(
            feed_input(&shared, Some("t1"), "\r"),
            json!({
                "echo": "\x08 \x08".repeat(3),
                "text": "/mx\r",
            })
        );
        // Control bytes still release the hold immediately: an abort signal
        // must reach the device without waiting for Enter. The chunk's own
        // local echo of "/m" precedes the erase.
        assert_eq!(
            feed_input(&shared, Some("t1"), "/m\x03"),
            json!({
                "echo": format!("/m{}", "\x08 \x08".repeat(2)),
                "text": "/m\x03",
            })
        );
    }

    #[test]
    fn input_interceptor_recognizes_corrected_line() {
        let shared = Shared::new();
        // Typo inside the prefix, then delete and retype: the final line must
        // still be recognized instead of leaking to the device.
        assert_eq!(
            feed_input(&shared, Some("t1"), "/mx"),
            json!({ "echo": "/mx", "suppress": true })
        );
        assert_eq!(
            feed_input(&shared, Some("t1"), "\x7f"),
            json!({ "echo": "\x08 \x08", "suppress": true })
        );
        assert_eq!(
            feed_input(&shared, Some("t1"), "c"),
            json!({ "echo": "c", "suppress": true })
        );
        // The corrected "/mc" line is consumed and executed.
        assert_eq!(
            feed_input(&shared, Some("t1"), "\r"),
            json!({ "echo": "\r\n", "suppress": true })
        );
    }

    #[test]
    fn input_interceptor_holds_and_releases_lines() {
        let shared = Shared::new();
        assert_eq!(
            feed_input(&shared, Some("t2"), "/mc"),
            json!({ "echo": "/mc", "suppress": true })
        );
        assert_eq!(
            feed_input(&shared, Some("t2"), " sta"),
            json!({ "echo": " sta", "suppress": true })
        );
        // Backspace erases the local echo once per held byte; seven of them
        // empty "/mc sta" again.
        assert_eq!(
            feed_input(&shared, Some("t2"), "\x7f\x7f\x7f\x7f\x7f\x7f\x7f"),
            json!({
                "echo": "\x08 \x08".repeat(7),
                "suppress": true,
            })
        );
        assert_eq!(
            feed_input(&shared, Some("t2"), "ok\r"),
            json!({ "text": "ok\r" })
        );
    }

    #[test]
    fn input_interceptor_buffers_are_per_terminal() {
        let shared = Shared::new();
        assert_eq!(
            feed_input(&shared, Some("t3"), "/m"),
            json!({ "echo": "/m", "suppress": true })
        );
        // A different terminal has a fresh buffer: the "x\r" passes through
        // there while t3 keeps holding.
        assert_eq!(
            feed_input(&shared, Some("t4"), "x\r"),
            json!({ "text": "x\r" })
        );
        assert_eq!(
            feed_input(&shared, Some("t3"), "c"),
            json!({ "echo": "c", "suppress": true })
        );
    }

    #[test]
    fn input_interceptor_drops_pairing_lf_after_consumed_line() {
        let shared = Shared::new();
        // CRLF Enter: the command is consumed on CR and the pairing LF must
        // not leak to the device as an empty command.
        assert_eq!(
            feed_input(&shared, Some("t5"), "/mc\r\n"),
            json!({ "echo": "/mc\r\n", "suppress": true })
        );
        // Unrelated lines replay verbatim, CRLF included.
        assert_eq!(
            feed_input(&shared, Some("t5"), "ok\r\n"),
            json!({ "text": "ok\r\n" })
        );
    }

    #[test]
    fn crlf_normalizes_bare_newlines() {
        assert_eq!(crlf("a\nb"), "a\r\nb");
        assert_eq!(crlf("a\r\nb"), "a\r\nb");
        assert_eq!(crlf("a\rb"), "a\rb");
        assert_eq!(crlf("line1\nline2\n"), "line1\r\nline2\r\n");
    }

    #[test]
    fn translates_status_views_with_the_active_locale() {
        let mut state = State::new();
        assert_eq!(render_status(&state), "Pick a baud rate, then click Apply.");
        state.messages = locale_json("zh-CN");
        assert_eq!(render_status(&state), "选择波特率后点击应用。");
        state.status = StatusView::NoSession;
        assert_eq!(
            render_status(&state),
            "✗ 未找到 BLE 会话，请先在终端中连接设备。"
        );
    }

    #[test]
    fn mc_help_uses_localized_title_and_lists_reboot() {
        let shared = Shared::new();
        {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.messages = locale_json("zh-CN");
        }
        let help = run_mc_command(&shared, None, "/mc").unwrap();
        assert!(help.starts_with("MicroCon 助手 命令："));
        assert!(help.contains("/mc reboot — 重新启动当前设备"));
        {
            let mut state = shared.state.lock().unwrap_or_else(|e| e.into_inner());
            state.messages = locale_json("en");
        }
        let help = run_mc_command(&shared, None, "/mc help").unwrap();
        assert!(help.starts_with("MicroCon Helper commands:"));
        assert!(help.contains("/mc reboot — restart the current device"));
        // The footer advertises the shorthand, and the abbreviated form
        // renders the identical help without any host round trip.
        assert!(help.contains("* First-letter abbreviations are accepted"));
        let shorthand = run_mc_command(&shared, None, "/mc h").unwrap();
        assert_eq!(shorthand, help);
    }

    #[test]
    fn mc_baud_validation_errors_and_shorthand_need_no_session() {
        let shared = Shared::new();
        // Validation runs before session selection, so these paths are
        // host-free and the abbreviated form must route identically.
        let usage = "Usage: /mc baud <rate> (1200–921600)";
        let out_of_range = "✗ Baud rate is out of range (1200–921600).";
        assert_eq!(
            run_mc_command(&shared, None, "/mc baud").unwrap_err(),
            usage
        );
        assert_eq!(
            run_mc_command(&shared, None, "/mc baud abc").unwrap_err(),
            usage
        );
        assert_eq!(
            run_mc_command(&shared, None, "/mc baud 100").unwrap_err(),
            out_of_range
        );
        assert_eq!(
            run_mc_command(&shared, None, "/mc b 1000000").unwrap_err(),
            out_of_range
        );
    }

    #[test]
    fn restart_wait_ignores_telemetry_and_resolves_on_exact_ack() {
        let shared = Shared::new();
        let command_id = 42;
        let (_generation, rx) = arm_status_wait(&shared, command_id, WaitMatch::AckIdOnly);
        let frame = |message: &Value| {
            let hex: String = message
                .to_string()
                .bytes()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            json!({ "data": hex })
        };
        // Device telemetry (id=0, baud matching the picker) must not resolve
        // a restart wait: that mis-parse showed the raw frame as the result.
        handle_ble_notification(
            &shared,
            &frame(&json!({"type": "status", "id": 0, "baud": 9600})),
        );
        assert!(rx.try_recv().is_err());
        let ack = json!({"type": "restart", "id": command_id, "ok": true});
        handle_ble_notification(&shared, &frame(&ack));
        let resolved = rx.try_recv().expect("the exact ack id must resolve");
        assert_eq!(resolved["ok"], json!(true));
    }
}
