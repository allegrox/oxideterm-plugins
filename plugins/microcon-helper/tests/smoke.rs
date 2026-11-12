// SPDX-License-Identifier: MIT
//! Protocol smoke test: spawns the compiled plugin binary, plays the host, and
//! asserts the plugin's protocol frames end to end (activate, session loading,
//! the set_baud and restart round trips, deactivate with subscription
//! release).

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::Duration;

struct Host {
    child: Child,
    stdin: ChildStdin,
    frames: Receiver<Value>,
    // Frames seen while matching a different expectation; background plugin
    // threads can emit out of order, so unmatched frames are kept for later
    // matchers instead of being dropped.
    backlog: std::sync::Mutex<Vec<Value>>,
}

impl Host {
    fn spawn() -> Host {
        let mut child = Command::new(env!("CARGO_BIN_EXE_oxideterm-microcon-helper"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn plugin binary");
        let reader = BufReader::new(child.stdout.take().expect("stdout"));
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in reader.lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                if tx
                    .send(serde_json::from_str(&line).expect("valid protocol frame"))
                    .is_err()
                {
                    break;
                }
            }
        });
        Host {
            stdin: child.stdin.take().expect("stdin"),
            child,
            frames: rx,
            backlog: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn send(&mut self, payload: &Value) -> String {
        let request_id = format!("smoke-{}", next_request());
        let mut inner = payload.clone();
        inner["requestId"] = json!(request_id);
        let envelope = json!({
            "protocolVersion": 1,
            "requestId": request_id,
            "payload": inner,
        });
        writeln!(self.stdin, "{envelope}").expect("write frame");
        request_id
    }

    fn reply(&mut self, request_id: &str, value: Value) {
        let envelope = json!({
            "protocolVersion": 1,
            "requestId": Value::Null,
            "payload": { "requestId": request_id, "result": { "status": "ok", "value": value } },
        });
        writeln!(self.stdin, "{envelope}").expect("write frame");
    }

    fn reply_error(&mut self, request_id: &str) {
        let envelope = json!({
            "protocolVersion": 1,
            "requestId": Value::Null,
            "payload": { "requestId": request_id, "result": { "status": "error",
                "error": { "code": "rejected", "message": "no such pane", "recoverable": false } } },
        });
        writeln!(self.stdin, "{envelope}").expect("write frame");
    }

    fn expect(&self, label: &str, matcher: impl Fn(&Value) -> bool) -> Value {
        let deadline = Duration::from_secs(5);
        let mut backlog = self.backlog.lock().unwrap();
        if let Some(index) = backlog.iter().position(&matcher) {
            return backlog.remove(index);
        }
        loop {
            match self.frames.recv_timeout(deadline) {
                Ok(frame) => {
                    if std::env::var("SMOKE_DEBUG").is_ok() {
                        eprintln!(
                            "[frame] {}",
                            serde_json::to_string(&frame).unwrap_or_default()
                        );
                    }
                    if matcher(&frame) {
                        return frame;
                    }
                    backlog.push(frame);
                }
                Err(RecvTimeoutError::Timeout) => panic!("timeout waiting for {label}"),
                Err(RecvTimeoutError::Disconnected) => panic!("plugin exited before {label}"),
            }
        }
    }

    fn call(&self, method: &str) -> Value {
        self.expect(method, |frame| {
            frame.pointer("/payload/type").and_then(Value::as_str) == Some("callHostApi")
                && frame.pointer("/payload/method").and_then(Value::as_str) == Some(method)
        })
    }

    fn response(&self, request_id: &str) -> Value {
        self.expect(request_id, |frame| {
            frame.pointer("/payload/requestId").and_then(Value::as_str) == Some(request_id)
                && frame.pointer("/payload/result").is_some()
        })
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn next_request() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::SeqCst) + 1
}

const SERVICE_UUID: &str = "b7e70001-b5a3-f393-e0a9-e50e24dcca9e";
const STATUS_UUID: &str = "b7e70003-b5a3-f393-e0a9-e50e24dcca9e";

fn event(name: &str, payload: Value) -> Value {
    json!({ "kind": { "type": "sendEvent", "event": { "name": name, "payload": payload } } })
}

fn schema_text(frame: &Value) -> String {
    frame
        .pointer("/payload/registration/metadata/schema")
        .map(|schema| schema.to_string())
        .unwrap_or_default()
}

#[test]
fn full_protocol_round_trip() {
    let mut host = Host::spawn();

    // activate answers ok immediately; getLocale and ble.sessions run after.
    let activate_id = host.send(&json!({ "kind": { "type": "activate" } }));
    let locale_call = host.call("getLocale");
    assert_eq!(
        locale_call
            .pointer("/payload/namespace")
            .and_then(Value::as_str),
        Some("app"),
        "locale comes from the app namespace"
    );
    assert!(
        locale_call
            .pointer("/requestId")
            .map(Value::is_null)
            .unwrap_or(true)
    );
    host.reply(
        locale_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!("zh-CN"),
    );

    // Registration frames arrive before the activation response; match in
    // emission order: subscription + input interceptor are discarded by the
    // tab matcher, then runtimeReady, then the response.
    let first_view = host.expect("tab registration", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
    });
    let schema = &first_view["payload"]["registration"]["metadata"]["schema"];
    assert_eq!(
        schema["title"],
        json!("MicroCon 助手"),
        "zh-CN locale applied"
    );
    // Control row: baud picker (default 9600), apply, spacer, restart.
    let controls = &schema["sections"][0]["controls"];
    assert_eq!(
        controls[0]["children"][0]["value"],
        json!("9600"),
        "picker defaults to the documented shipping baud rate"
    );
    assert_eq!(controls[0]["children"][1]["id"], json!("apply"));
    assert_eq!(controls[0]["children"][2]["kind"], json!("spacer"));
    assert_eq!(controls[0]["children"][3]["id"], json!("restart"));

    host.expect("runtimeReady", |frame| {
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("runtimeReady")
    });

    let activated = host.response(&activate_id);
    assert_eq!(
        activated.pointer("/payload/result/value/activated"),
        Some(&json!(true))
    );

    let sessions_call = host.call("sessions");
    host.reply(
        sessions_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "sessions": [
            { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
            { "sessionId": "s2", "deviceAddress": "AA:BB", "connected": false },
        ] }),
    );
    // With no explicit selection the plugin consults the focused terminal
    // (implicit pick) before rendering the refreshed list.
    let metadata_call = host.call("getMetadata");
    assert_eq!(
        metadata_call.pointer("/payload/namespace"),
        Some(&json!("terminal"))
    );
    host.reply(
        metadata_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    host.expect("tab refresh with sessions", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
    });

    // apply: pick 115200 → subscribe → read current baud → write set_baud →
    // status reply resolved through the notification slot.
    host.send(&event(
        "ui.event",
        json!({ "controlId": "baud", "type": "change", "value": "115200" }),
    ));
    host.send(&event(
        "ui.event",
        json!({ "controlId": "apply", "type": "click" }),
    ));
    let subscribe = host.call("onNotification");
    assert_eq!(
        subscribe.pointer("/payload/args/characteristicUuid"),
        Some(&json!(STATUS_UUID))
    );
    assert_eq!(
        subscribe.pointer("/payload/args/serviceUuid"),
        Some(&json!(SERVICE_UUID))
    );
    host.reply(
        subscribe
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );

    let read_call = host.call("read");
    assert_eq!(
        read_call.pointer("/payload/args/characteristicUuid"),
        Some(&json!(STATUS_UUID))
    );
    host.reply(
        read_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "data": "{\"baud\":9600,\"batteryPercent\":88}" }),
    );

    let write_call = host.call("write");
    let args = &write_call["payload"]["args"];
    let command: Value =
        serde_json::from_str(args["data"].as_str().unwrap().trim()).expect("set_baud JSON");
    assert_eq!(command["type"], json!("set_baud"));
    assert_eq!(command["proto"], json!(1));
    assert_eq!(command["baud"], json!(115200), "picker selection applied");
    assert_eq!(
        command["expectedBaud"],
        json!(9600),
        "current baud read from the device"
    );
    assert_eq!(args["dataEncoding"], json!("utf8"));
    assert_eq!(args["writeType"], json!("withResponse"));
    assert_eq!(args["sessionId"], json!("s1"));
    host.reply(
        write_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );

    let status_frame = format!(
        "{}\n",
        json!({ "type": "status", "baud": 115200, "batteryPercent": 88, "tempValid": true, "tempC": 31.5 })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(status_frame.as_bytes()) }),
    ));
    let done = host.expect("sentOk status", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("✓ 9600 → 115200")
    });
    let status_text = done
        .pointer("/payload/registration/metadata/schema/sections/0/controls/1/text")
        .and_then(Value::as_str)
        .unwrap();
    assert_eq!(status_text, "✓ 9600 → 115200");

    // restart: the subscription is already live, so only the session lookup
    // and the write go out; the device acks the exact command id and then
    // drops the link.
    host.send(&event(
        "ui.event",
        json!({ "controlId": "restart", "type": "click" }),
    ));
    let sessions_call = host.call("sessions");
    host.reply(
        sessions_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "sessions": [
            { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
        ] }),
    );
    let metadata_call = host.call("getMetadata");
    host.reply(
        metadata_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let write_call = host.call("write");
    let args = &write_call["payload"]["args"];
    let command: Value =
        serde_json::from_str(args["data"].as_str().unwrap().trim()).expect("restart JSON");
    assert_eq!(command["proto"], json!(1));
    assert_eq!(command["type"], json!("restart"));
    assert_eq!(command["confirm"], json!("restart"));
    assert!(command["id"].is_number(), "acked by exact command id");
    assert_eq!(args["sessionId"], json!("s1"));
    host.reply(
        write_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let ack_frame = format!(
        "{}\n",
        json!({ "type": "result", "id": command["id"], "ok": true })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(ack_frame.as_bytes()) }),
    ));
    host.expect("restart ack", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("设备已重启")
    });

    // Overlay lifecycle: the plugin owns the refresh loop. Opening binds the
    // focused pane's session and issues a fresh get_status (which also
    // un-parks the telemetry the restart cleared); while the overlay is
    // visible the pushed schema carries the status row.
    host.send(&event(
        "toolbar.overlayOpened",
        json!({ "actionId": "mc-helper-mode", "paneId": 1 }),
    ));
    // The bind resolves the implicit pick (sessions + getMetadata); the
    // opening poll reuses the bound session id, so only a session lookup goes
    // out and focus can no longer move the binding.
    for expect_metadata in [true, false] {
        let sessions_call = host.call("sessions");
        host.reply(
            sessions_call
                .pointer("/payload/requestId")
                .and_then(Value::as_str)
                .unwrap(),
            json!({ "sessions": [
                { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
            ] }),
        );
        if expect_metadata {
            let metadata_call = host.call("getMetadata");
            host.reply(
                metadata_call
                    .pointer("/payload/requestId")
                    .and_then(Value::as_str)
                    .unwrap(),
                json!({}),
            );
        }
    }
    let status_write = host.call("write");
    let status_command: Value = serde_json::from_str(
        status_write
            .pointer("/payload/args/data")
            .and_then(Value::as_str)
            .unwrap()
            .trim(),
    )
    .expect("get_status JSON");
    assert_eq!(status_command["type"], json!("get_status"));
    host.reply(
        status_write
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let telemetry_frame = format!(
        "{}\n",
        json!({ "type": "status", "id": status_command["id"], "baud": 9600, "batteryPercent": 88, "tempValid": true, "tempC": 31.5 })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(telemetry_frame.as_bytes()) }),
    ));
    let opened = host.expect("overlay status row", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("panelStatusRow")
    });
    let schema = &opened["payload"]["registration"]["metadata"]["schema"];
    assert!(
        schema.get("panelStatus").is_none(),
        "the host no longer reads a panelStatus field; the plugin must not send it"
    );
    let row = &schema["sections"][0]["controls"][2];
    assert_eq!(row["id"], json!("panelStatusRow"));
    assert_eq!(row["children"][0]["icon"], json!("square-activity"));
    assert_eq!(row["children"][0]["tooltip"], json!("当前波特率"));
    assert_eq!(row["children"][0]["text"], json!("9600"));
    assert_eq!(row["children"][1]["icon"], json!("battery-full"));
    assert_eq!(row["children"][1]["tooltip"], json!("电池剩余电量"));
    assert_eq!(row["children"][1]["text"], json!("88%"));
    assert_eq!(row["children"][2]["icon"], json!("thermometer"));
    assert_eq!(row["children"][2]["tooltip"], json!("设备温度"));
    assert_eq!(row["children"][2]["text"], json!("31.5°C"));
    assert_eq!(row["children"][3]["kind"], json!("spacer"));
    assert_eq!(row["children"][4]["text"], json!("10s"));

    // Closing drops the binding and the row; the ticker retires at its next
    // tick without polling again.
    host.send(&event(
        "toolbar.overlayClosed",
        json!({ "actionId": "mc-helper-mode", "paneId": 1, "reason": "released" }),
    ));
    let closed = host.expect("overlay closed schema", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && !schema_text(frame).contains("panelStatusRow")
    });
    assert_eq!(
        closed
            .pointer("/payload/registration/metadata/schema/sections/0/controls")
            .and_then(Value::as_array)
            .map(|controls| controls.len()),
        Some(2),
        "status row removed once the overlay closed"
    );

    // Reopen pane 1 to populate its cache, then hand the card away and back:
    // closed(rebound) must not republish and the follow-up opened repaints
    // from cache before the refresh round trip completes. Drop the frames of
    // the released overlay first so only reopen frames can match below.
    host.backlog.lock().unwrap().clear();
    host.send(&event(
        "toolbar.overlayOpened",
        json!({ "actionId": "mc-helper-mode", "paneId": 1 }),
    ));
    for expect_metadata in [true, false] {
        let sessions_call = host.call("sessions");
        host.reply(
            sessions_call
                .pointer("/payload/requestId")
                .and_then(Value::as_str)
                .unwrap(),
            json!({ "sessions": [
                { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
            ] }),
        );
        if expect_metadata {
            let metadata_call = host.call("getMetadata");
            host.reply(
                metadata_call
                    .pointer("/payload/requestId")
                    .and_then(Value::as_str)
                    .unwrap(),
                json!({}),
            );
        }
    }
    let status_write = host.call("write");
    let status_command: Value = serde_json::from_str(
        status_write
            .pointer("/payload/args/data")
            .and_then(Value::as_str)
            .unwrap()
            .trim(),
    )
    .expect("get_status JSON");
    assert_eq!(status_command["type"], json!("get_status"));
    host.reply(
        status_write
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let telemetry_frame = format!(
        "{}\n",
        json!({ "type": "status", "id": status_command["id"], "baud": 9600, "batteryPercent": 88, "tempValid": false })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(telemetry_frame.as_bytes()) }),
    ));
    host.expect("overlay cache populated", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("panelStatusRow")
            && schema_text(frame).contains("88%")
    });

    // Rebound handover: let the superseded ticker retire through its unbound
    // fallback first, then drop everything up to that point so only frames
    // produced by the reopen can match the cached repaint.
    host.send(&event(
        "toolbar.overlayClosed",
        json!({ "actionId": "mc-helper-mode", "paneId": 1, "reason": "rebound" }),
    ));
    std::thread::sleep(Duration::from_millis(1500));
    host.backlog.lock().unwrap().clear();
    host.send(&event(
        "toolbar.overlayOpened",
        json!({ "actionId": "mc-helper-mode", "paneId": 1 }),
    ));
    // The session capture runs before the repaint, so answer the implicit
    // pick; the cached schema must then arrive while the refresh poll is
    // still waiting for its write reply, i.e. no new telemetry was delivered.
    let sessions_call = host.call("sessions");
    host.reply(
        sessions_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "sessions": [
            { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
        ] }),
    );
    let metadata_call = host.call("getMetadata");
    host.reply(
        metadata_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let cached = host.expect("rebound cached repaint", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("panelStatusRow")
            && schema_text(frame).contains("88%")
    });
    let cached_row =
        &cached["payload"]["registration"]["metadata"]["schema"]["sections"][0]["controls"][2];
    assert_eq!(cached_row["children"][1]["text"], json!("88%"));
    assert_eq!(
        cached_row["children"][4]["text"],
        json!("10s"),
        "the countdown restarts from a full cycle on reopen"
    );

    // The refresh poll still runs to update the cache: answer it with
    // changed telemetry and the row follows the device.
    let sessions_call = host.call("sessions");
    host.reply(
        sessions_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "sessions": [
            { "sessionId": "s1", "deviceName": "MicroCon-A", "connected": true },
        ] }),
    );
    let refresh_write = host.call("write");
    let refresh_command: Value = serde_json::from_str(
        refresh_write
            .pointer("/payload/args/data")
            .and_then(Value::as_str)
            .unwrap()
            .trim(),
    )
    .expect("get_status JSON");
    assert_eq!(refresh_command["type"], json!("get_status"));
    host.reply(
        refresh_write
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let refreshed_frame = format!(
        "{}\n",
        json!({ "type": "status", "id": refresh_command["id"], "baud": 9600, "batteryPercent": 91, "tempValid": false })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(refreshed_frame.as_bytes()) }),
    ));
    host.expect("rebound refresh applied", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("91%")
    });

    // Release pane 1 for good so the ticker retires before the chip-click
    // regression below re-enters the overlay state.
    host.send(&event(
        "toolbar.overlayClosed",
        json!({ "actionId": "mc-helper-mode", "paneId": 1, "reason": "released" }),
    ));
    host.expect("overlay released schema", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && !schema_text(frame).contains("panelStatusRow")
    });

    // Jitter regression: cycling the chip into the overlay state must
    // register the status row schema before the reportActionState frame, so
    // the host's first overlay paint already has the final height. Drop the
    // frames the ticker left in the backlog so only the click's frames are
    // inspected.
    host.backlog.lock().unwrap().clear();
    host.send(&event(
        "toolbar.actionClicked",
        json!({ "actionId": "mc-helper-mode", "paneId": 1, "currentState": "inline" }),
    ));
    let saw_row = std::cell::Cell::new(false);
    let report_call = host.expect("overlay reportActionState", |frame| {
        if frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
            && schema_text(frame).contains("panelStatusRow")
        {
            saw_row.set(true);
            return false;
        }
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("callHostApi")
            && frame.pointer("/payload/method").and_then(Value::as_str) == Some("reportActionState")
    });
    assert!(
        saw_row.get(),
        "the status row schema must be registered before reportActionState"
    );
    assert_eq!(
        report_call.pointer("/payload/args/stateId"),
        Some(&json!("overlay"))
    );
    host.reply(
        report_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );

    // health + deactivate. The subscription release (offNotification for the
    // status characteristic) happens before the ack; the binary exits after
    // answering.
    let health_id = host.send(&json!({ "kind": { "type": "health" } }));
    let health = host.response(&health_id);
    assert_eq!(
        health.pointer("/payload/result/value/ok"),
        Some(&json!(true))
    );

    let stop_id = host.send(&json!({ "kind": { "type": "deactivate" } }));
    let off = host.call("offNotification");
    assert_eq!(off.pointer("/payload/args/sessionId"), Some(&json!("s1")));
    assert_eq!(
        off.pointer("/payload/args/characteristicUuid"),
        Some(&json!(STATUS_UUID))
    );
    host.reply(
        off.pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let stopped = host.response(&stop_id);
    assert_eq!(
        stopped.pointer("/payload/result/value/stopped"),
        Some(&json!(true))
    );
    let exit_code = host.child.wait().expect("wait for exit");
    assert_eq!(exit_code.code(), Some(0), "deactivate exits cleanly");
}

#[test]
fn mc_command_round_trip() {
    let mut host = Host::spawn();

    // activate answers ok immediately; getLocale and ble.sessions run after.
    let activate_id = host.send(&json!({ "kind": { "type": "activate" } }));
    let locale_call = host.call("getLocale");
    host.reply(
        locale_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!("en"),
    );
    host.expect("runtimeReady", |frame| {
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("runtimeReady")
    });
    host.response(&activate_id);
    let sessions_call = host.call("sessions");
    assert_eq!(
        sessions_call.pointer("/payload/namespace"),
        Some(&json!("ble"))
    );
    host.reply(
        sessions_call.pointer("/payload/requestId").and_then(Value::as_str).unwrap(),
        json!({ "sessions": [ { "sessionId": "t9", "deviceName": "MicroCon-X", "connected": true } ] }),
    );
    // The interceptor registration must be announced for the hook to fire.
    host.expect("input interceptor registration", |frame| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("terminal-input-interceptor")
            && frame.pointer("/payload/registration/metadata/command")
                == Some(&json!("microconHelper.input"))
    });

    // A partial "/mc" prefix is held back with a screen-only local echo.
    let hold_id = host.send(&json!({
        "kind": { "type": "dispatchCommand", "command": "microconHelper.input",
            "args": { "registrationId": "terminal-input-interceptor", "terminalId": "t9", "data": "/m" } }
    }));
    let held = host.response(&hold_id);
    assert_eq!(
        held.pointer("/payload/result/value"),
        Some(&json!({ "echo": "/m", "suppress": true })),
        "held prefix suppressed with local echo"
    );

    // An unrelated line keeps holding with local echo until its terminator,
    // then the local echo (including the held "/m") is erased and the whole
    // line is replayed verbatim to the device.
    let pass_id = host.send(&json!({
        "kind": { "type": "dispatchCommand", "command": "microconHelper.input",
            "args": { "registrationId": "terminal-input-interceptor", "terminalId": "t9", "data": "ls\r" } }
    }));
    let passed = host.response(&pass_id);
    assert_eq!(
        passed.pointer("/payload/result/value/text"),
        Some(&json!("/mls\r"))
    );
    assert_eq!(
        passed.pointer("/payload/result/value/echo"),
        Some(&json!(format!("ls{}", "\x08 \x08".repeat(4)))),
        "local echo erased before replay"
    );

    // /mc status executes: the response echoes the line locally and suppresses
    // it, the executor resolves the session by terminalId (no implicit
    // getMetadata hop) and reports through terminal.writeLocal.
    let mc_id = host.send(&json!({
        "kind": { "type": "dispatchCommand", "command": "microconHelper.input",
            "args": { "registrationId": "terminal-input-interceptor", "terminalId": "t9", "data": "/mc status\r" } }
    }));
    let mc = host.response(&mc_id);
    assert_eq!(
        mc.pointer("/payload/result/value"),
        Some(&json!({ "echo": "/mc status\r\n", "suppress": true })),
        "/mc line consumed with local echo"
    );

    let resolve_call = host.call("sessions");
    host.reply(
        resolve_call.pointer("/payload/requestId").and_then(Value::as_str).unwrap(),
        json!({ "sessions": [ { "sessionId": "t9", "deviceName": "MicroCon-X", "connected": true } ] }),
    );
    let read_call = host.call("read");
    assert_eq!(
        read_call.pointer("/payload/args/sessionId"),
        Some(&json!("t9"))
    );
    assert_eq!(
        read_call.pointer("/payload/args/characteristicUuid"),
        Some(&json!(STATUS_UUID))
    );
    host.reply(
        read_call.pointer("/payload/requestId").and_then(Value::as_str).unwrap(),
        json!({ "data": "{\"baud\":115200,\"batteryPercent\":77,\"tempValid\":true,\"tempC\":30.0}" }),
    );
    let notify = host.expect("terminal.writeLocal", |frame| {
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("callHostApi")
            && frame.pointer("/payload/namespace").and_then(Value::as_str) == Some("terminal")
            && frame.pointer("/payload/method").and_then(Value::as_str) == Some("writeLocal")
    });
    // The reply must target the terminal the command was typed in, not the
    // (possibly moved) active pane.
    assert_eq!(
        notify.pointer("/payload/args/terminalId"),
        Some(&json!("t9")),
        "writeLocal must carry the intercepted terminalId"
    );
    let description = notify
        .pointer("/payload/args/text")
        .and_then(Value::as_str)
        .expect("writeLocal text");
    // The command line is closed by the hook's terminator echo; the display
    // channel must carry only the reply.
    assert!(
        !description.contains("/mc status"),
        "reply must not repeat the command line: {description}"
    );
    // Field order is part of the /mc status contract: baud, battery, temp.
    assert!(
        description.contains("Current baud: 115200"),
        "status rendered: {description}"
    );
    assert!(
        description.contains("Battery: 77%"),
        "status rendered: {description}"
    );
    assert!(
        description.contains("Device temp: 30°C"),
        "status rendered: {description}"
    );
    let baud_end = description.find("Current baud: 115200").unwrap();
    let battery_start = description.find("Battery: 77%").unwrap();
    let temp_start = description.find("Device temp: 30°C").unwrap();
    assert!(
        baud_end < battery_start && battery_start < temp_start,
        "status fields out of order: {description}"
    );
    host.reply(
        notify
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!(true),
    );

    // /mc baud drives the full set_baud chain against the same session.
    let baud_id = host.send(&json!({
        "kind": { "type": "dispatchCommand", "command": "microconHelper.input",
            "args": { "registrationId": "terminal-input-interceptor", "terminalId": "t9", "data": "/mc baud 230400\r" } }
    }));
    let baud = host.response(&baud_id);
    assert_eq!(
        baud.pointer("/payload/result/value"),
        Some(&json!({ "echo": "/mc baud 230400\r\n", "suppress": true })),
        "/mc baud line consumed with local echo"
    );

    let resolve_call = host.call("sessions");
    host.reply(
        resolve_call.pointer("/payload/requestId").and_then(Value::as_str).unwrap(),
        json!({ "sessions": [ { "sessionId": "t9", "deviceName": "MicroCon-X", "connected": true } ] }),
    );
    let subscribe = host.call("onNotification");
    assert_eq!(
        subscribe.pointer("/payload/args/sessionId"),
        Some(&json!("t9"))
    );
    host.reply(
        subscribe
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );

    let read_call = host.call("read");
    assert_eq!(
        read_call.pointer("/payload/args/sessionId"),
        Some(&json!("t9"))
    );
    host.reply(
        read_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "data": "{\"baud\":115200}" }),
    );
    let write_call = host.call("write");
    let args = &write_call["payload"]["args"];
    assert_eq!(args["sessionId"], json!("t9"));
    assert_eq!(
        args["characteristicUuid"],
        json!("b7e70002-b5a3-f393-e0a9-e50e24dcca9e")
    );
    let command: Value =
        serde_json::from_str(args["data"].as_str().unwrap().trim()).expect("set_baud JSON");
    assert_eq!(command["type"], json!("set_baud"));
    assert_eq!(command["baud"], json!(230400));
    assert_eq!(command["expectedBaud"], json!(115200));
    host.reply(
        write_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );

    // The device acks with the exact command id; that must resolve the wait.
    let ack = format!(
        "{}\n",
        json!({ "type": "set_baud_ok", "id": command["id"], "baud": 230400 })
    );
    host.send(&event(
        "ble.notification",
        json!({ "characteristicUuid": STATUS_UUID, "data": hex_encode(ack.as_bytes()) }),
    ));
    let notify = host.expect("terminal.writeLocal after baud", |frame| {
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("callHostApi")
            && frame.pointer("/payload/namespace").and_then(Value::as_str) == Some("terminal")
            && frame.pointer("/payload/method").and_then(Value::as_str) == Some("writeLocal")
    });
    let description = notify
        .pointer("/payload/args/text")
        .and_then(Value::as_str)
        .expect("writeLocal text");
    assert!(
        description.contains("Baud rate set to 230400"),
        "baud rendered: {description}"
    );
    assert!(
        description.contains("MicroCon-X"),
        "target rendered: {description}"
    );
    host.reply(
        notify
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!(true),
    );

    // deactivate releases the /mc baud subscription (offNotification) before
    // answering; the binary exits after the release is acknowledged.
    let stop_id = host.send(&json!({ "kind": { "type": "deactivate" } }));
    let off = host.call("offNotification");
    assert_eq!(off.pointer("/payload/args/sessionId"), Some(&json!("t9")));
    assert_eq!(
        off.pointer("/payload/args/characteristicUuid"),
        Some(&json!(STATUS_UUID))
    );
    host.reply(
        off.pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({}),
    );
    let stopped = host.response(&stop_id);
    assert_eq!(
        stopped.pointer("/payload/result/value/stopped"),
        Some(&json!(true))
    );
    let exit_code = host.child.wait().expect("wait for exit");
    assert_eq!(exit_code.code(), Some(0), "deactivate exits cleanly");
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn toolbar_overlay_rejection_rolls_back_placeholder_row() {
    let mut host = Host::spawn();
    let activate_id = host.send(&json!({ "kind": { "type": "activate" } }));
    let locale_call = host.call("getLocale");
    host.reply(
        locale_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!("en"),
    );
    host.expect("runtimeReady", |frame| {
        frame.pointer("/payload/type").and_then(Value::as_str) == Some("runtimeReady")
    });
    let _ = host.response(&activate_id);
    let sessions_call = host.call("sessions");
    host.reply(
        sessions_call
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
        json!({ "sessions": [] }),
    );

    let is_tab = |frame: &Value| {
        frame
            .pointer("/payload/registration/kind")
            .and_then(Value::as_str)
            == Some("tab")
    };
    let has_status_row = |frame: &Value| schema_text(frame).contains("\"readout\"");

    // inline -> overlay: the plugin pre-registers the schema with the
    // placeholder status row before reporting the state, so an accepted
    // report renders a full-height card.
    host.send(&event(
        "toolbar.actionClicked",
        json!({ "actionId": "mc-helper-mode", "paneId": 1, "currentState": "inline" }),
    ));
    host.expect("placeholder status registration", |frame| {
        is_tab(frame) && has_status_row(frame)
    });
    let report = host.call("reportActionState");
    assert_eq!(
        report.pointer("/payload/args/stateId"),
        Some(&json!("overlay")),
        "the click cycles inline to overlay"
    );

    // The host refuses the state: the plugin must undo the placeholder row,
    // otherwise the tab keeps a frozen status row with no overlay ticker.
    host.reply_error(
        report
            .pointer("/payload/requestId")
            .and_then(Value::as_str)
            .unwrap(),
    );
    // Read the live stream directly: the pre-click tab registrations are
    // already parked in the expect() backlog, so a tab frame without the
    // status row can only be the rollback if it arrives now.
    loop {
        let frame = host
            .frames
            .recv_timeout(Duration::from_secs(5))
            .expect("plugin stream alive");
        if is_tab(&frame) && !has_status_row(&frame) {
            break;
        }
    }
}
