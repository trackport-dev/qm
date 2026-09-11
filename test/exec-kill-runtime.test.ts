import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import { existsSync, mkdirSync, rmdirSync, rmSync } from "node:fs";
import { test } from "node:test";
import { killableScript, killScript, pgidMarkerPath } from "../src/sandbox/exec-kill.ts";

const linux = process.platform === "linux";
const shell = (script: string) => spawnSync("/bin/sh", ["-c", script], { encoding: "utf8", timeout: 5_000 });

test(
  "executor cancellation stops a launched group and retains the launch fence",
  { skip: !linux, timeout: 10_000 },
  async (t) => {
    const uid = randomUUID();
    const marker = pgidMarkerPath(uid);
    const cancelled = `${marker}.cancelled`;
    const child = spawn("/bin/sh", ["-c", killableScript("printf READY; sleep 30", uid)]);
    const closed = once(child, "close");
    t.after(async () => {
      if (child.exitCode === null && child.signalCode === null) shell(killScript(uid));
      await closed;
      rmSync(marker, { force: true });
      rmSync(cancelled, { force: true });
    });
    const [output] = await Promise.race([
      once(child.stdout, "data"),
      closed.then(() => assert.fail("command exited before readiness")),
    ]);
    assert.equal(String(output), "READY");
    assert.equal(existsSync(marker), true);
    const stopped = shell(killScript(uid));
    assert.equal(stopped.error, undefined);
    assert.equal(stopped.status, 0, stopped.stderr);
    const [code, signal] = await closed;
    assert.equal(code, null);
    assert.equal(signal, "SIGKILL");
    assert.equal(existsSync(cancelled), true);
    assert.equal(existsSync(marker), false);
  },
);

test("executor refuses to run if its process group cannot be recorded", { skip: !linux }, (t) => {
  const uid = randomUUID();
  const marker = pgidMarkerPath(uid);
  mkdirSync(marker);
  t.after(() => rmdirSync(marker));
  const result = shell(killableScript("printf SHOULD_NOT_EXECUTE", uid));
  assert.equal(result.error, undefined);
  assert.equal(result.status, 125);
  assert.equal(result.stdout, "");
});

test(
  "executor cancellation survives completion of the kill request and rejects delayed launches",
  { skip: !linux },
  (t) => {
    const uid = randomUUID();
    const marker = pgidMarkerPath(uid);
    const cancelled = `${marker}.cancelled`;
    t.after(() => {
      rmSync(marker, { force: true });
      rmSync(cancelled, { force: true });
    });
    const stopped = shell(killScript(uid));
    assert.equal(stopped.error, undefined);
    assert.equal(stopped.status, 0, stopped.stderr);
    assert.equal(existsSync(cancelled), true);
    assert.equal(existsSync(marker), false);
    for (let attempt = 0; attempt < 2; attempt++) {
      const late = shell(killableScript("printf SHOULD_NOT_EXECUTE", uid));
      assert.equal(late.error, undefined);
      assert.equal(late.status, 130, late.stderr);
      assert.equal(late.stdout, "");
      assert.equal(existsSync(marker), false);
      assert.equal(existsSync(cancelled), true);
    }
  },
);

test(
  "uncancelled executor commands preserve output and exit status and remove their group marker",
  { skip: !linux },
  (t) => {
    const uid = randomUUID();
    const marker = pgidMarkerPath(uid);
    t.after(() => rmSync(marker, { force: true }));
    const result = shell(killableScript("printf EXECUTED; sh -c 'exit 23'", uid));
    assert.equal(result.error, undefined);
    assert.equal(result.status, 23, result.stderr);
    assert.equal(result.stdout, "EXECUTED");
    assert.equal(existsSync(marker), false);
    assert.equal(existsSync(`${marker}.cancelled`), false);
  },
);
