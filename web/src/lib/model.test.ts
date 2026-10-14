import { describe, expect, it } from "vitest";
import type { AppPlan, DestinationList, Node, Operation } from "./api";
import { activity, cell, day, paths, problems, setup, short, span, summary, tested, when } from "./model";
import { app, destination, destinations, node, plan } from "../test/fixtures";

const now = Date.parse("2026-10-07T08:01:00Z");
const { last_drill: _drill, ...rest } = app;
const copied: AppPlan = { ...rest, state: "protected", limit_codes: [], targets: [{ destination_id: "d1", selected: true, state: "protected", due_at: "2026-10-08T03:00:00Z", last_good: { completed_at: "2026-10-07T03:12:00Z" } }] };
const online: Node = { ...node, last_seen_at: "2026-10-07T08:00:30Z" };

describe("times", () => {
  it("reads times compactly", () => {
    expect(day("2026-10-07T03:00:00Z", now)).toBe("today");
    expect(day("2026-10-08T03:00:00Z", now)).toBe("tomorrow");
    expect(when("2026-10-01T03:12:00Z", now)).toBe("03:12 Oct 01");
    expect(when("2026-10-07T03:12:00Z", now)).toBe("03:12 today");
    expect(short("2026-10-07T03:12:00Z", now)).toBe("03:12");
    expect(short("2026-10-06T03:12:00Z", now)).toBe("Oct 06");
    expect(when(undefined, now)).toBe("never");
    expect(span(3 * 86_400_000 + 5)).toBe("3d");
    expect(span(5 * 3_600_000)).toBe("5h");
  });
});

describe("setup", () => {
  it("counts the steps that are done and names the next one", () => {
    expect(setup([], [], { ...plan, apps: [] })).toMatchObject({ done: 0, next: "devices", complete: false });
    expect(setup([{ ...node, approved_at: null }], [], null).next).toBe("devices");
    expect(setup([node], [], { ...plan, apps: [{ ...app, enabled: false }] })).toMatchObject({ done: 1, next: "storage" });
    expect(setup([node], [destination], { ...plan, apps: [{ ...app, enabled: false }] })).toMatchObject({ done: 2, next: "schedule" });
    expect(setup([node], [destination], plan)).toMatchObject({ done: 3, next: null, complete: true });
  });
});

describe("the header line", () => {
  it("counts restorable apps and names the newest snapshot and the next run", () => {
    expect(summary({ ...plan, apps: [copied, { ...app, targets: [] }] }, now)).toEqual({ restorable: 1, total: 2, newest: "2026-10-07T03:12:00Z", next: "2026-10-08T03:00:00Z" });
  });
});

describe("the recovery matrix", () => {
  it("shows the state and the newest snapshot of an app in one storage", () => {
    expect(cell(copied, "d1", now)).toMatchObject({ mark: { tone: "ok", label: "Restorable" }, value: "03:12" });
    expect(cell(copied, "d9", now)).toMatchObject({ mark: null, title: "Not scheduled to this storage" });
  });
  it("shows a failed restore test before an older passed one", () => {
    expect(tested(app, now)).toMatchObject({ mark: { tone: "fail" }, value: "Oct 01" });
    expect(tested(copied, now)).toMatchObject({ mark: { tone: "ok" }, value: "Oct 01" });
    expect(tested({ ...copied, restore_tested_at: undefined as never }, now)).toMatchObject({ mark: { tone: "unc" }, value: "untested" });
  });
});

describe("attention", () => {
  it("names an offline device and the age of its oldest last snapshot", () => {
    const away: Node = { ...node, last_seen_at: "2026-10-04T08:00:00Z" };
    expect(problems({ ...plan, apps: [copied] }, [away], destinations, now)).toContainEqual(expect.objectContaining({ subject: "odin", condition: "offline 3d", detail: "notes last snapshot 03:12." }));
  });
  it("keeps the failure code and adds one short line", () => {
    const failed: AppPlan = { ...copied, state: "failed", targets: [{ ...copied.targets[0]!, failure_code: "database_dump_failed" }] };
    expect(problems({ ...plan, apps: [failed] }, [online], destinations, now)[0]).toMatchObject({ tone: "fail", subject: "notes", condition: "database_dump_failed", code: true, detail: "Backup disk: Database dump failed." });
  });
  it("reports a failed restore test, a failed check, a device to approve, and a full disk", () => {
    const listing: DestinationList = { ...destinations, checks: [{ id: "c1", node_id: "n1", destination_id: "d1", state: "failed", result: { failure_code: "integrity_check_failed" } }], space: [{ destination_id: "d1", known: true, total_bytes: 100, available_bytes: 5 }] };
    const found = problems(plan, [online, { ...node, id: "n9", name: "pi", approved_at: null }], listing, now).map((p) => `${p.subject} ${p.condition}`);
    expect(found).toEqual(expect.arrayContaining(["notes drill_container_stopped", "Backup disk integrity_check_failed", "pi not approved", "Backup disk 95% full", "notes app_consistency_unverified"]));
  });
  it("finds nothing when everything is fine", () => {
    expect(problems({ ...plan, apps: [copied] }, [online], destinations, now)).toEqual([]);
  });
});

describe("activity and paths", () => {
  it("reads an operation as columns", () => {
    const names = { app: () => "notes", node: () => "odin", storage: () => "Backup disk" };
    const op = { id: "o1", app_id: "a1", kind: "backup", status: "succeeded", node_id: "n1", actor_id: "schedule:x", failure_code: "", finished_at: "2026-10-07T03:00:41Z", created_at: "2026-10-07T03:00:00Z", updated_at: "", result: { destination_id: "d1" } } as Operation;
    expect(activity(op, names)).toEqual({ kind: "backup", subject: "notes", target: "Backup disk", result: "ok", tone: "ok", took: 41_000 });
    expect(activity({ ...op, kind: "restore_drill" }, names)).toMatchObject({ kind: "restore test", target: "odin", result: "passed" });
    expect(activity({ ...op, kind: "restore", status: "failed" }, names)).toMatchObject({ kind: "restore", result: "failed", tone: "fail" });
  });
  it("sorts paths into included, dumped, excluded, and never", () => {
    const rows = paths({ ...app.app, definition: { ...app.app.definition, containers: [{ id: "c", name: "db", image: "mariadb", database: "mariadb" }] } }, ["/srv/media"]);
    expect(rows.map((r) => [r.status, r.label, r.reason])).toEqual([
      ["included", "/srv/notes", ""],
      ["included", "/srv/media", "Included by you"],
      ["dumped", "db", "MariaDB dump"],
      ["never", "/var/run/docker.sock", "host socket"],
    ]);
  });
});
