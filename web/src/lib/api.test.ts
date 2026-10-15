import { afterEach, expect, it, vi } from "vitest";
import { download, fileURL } from "./api";
afterEach(() => vi.unstubAllGlobals());
it("encodes saved paths without putting them into the route", () => {
  const url = new URL(fileURL("a/b", "files", { path: "/srv/a?b&c#d", snapshot: "s1" }), "https://synod.test");
  expect(url.pathname).toBe("/v1/apps/a%2Fb/files");
  expect(url.searchParams.get("path")).toBe("/srv/a?b&c#d");
});
it("only releases a complete verified download", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("saved", { headers: { "X-Synod-Transfer": "complete", "Content-Length": "5", "X-Synod-Format": "tar" } })));
  const result = await download("a1", { path: "/srv/notes" }, new AbortController().signal);
  expect(result.blob.size).toBe(5); expect(result.archive).toBe(true);
  expect(fetch).toHaveBeenCalledWith(expect.stringContaining("&verified=1"), expect.objectContaining({ credentials: "same-origin" }));
});
it.each([
  { "Content-Length": "5" },
  { "X-Synod-Transfer": "complete", "Content-Length": "6" },
])("rejects unconfirmed or truncated content", async (headers) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("saved", { headers: headers as HeadersInit })));
  await expect(download("a1", {}, new AbortController().signal)).rejects.toMatchObject({ detail: { code: "file_transfer_incomplete" } });
});
it("keeps actionable download errors", async () => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ code: "download_too_large", message: "Use synod file get.", request_id: "r1" }), { status: 413 })));
  await expect(download("a1", {}, new AbortController().signal)).rejects.toMatchObject({ detail: { code: "download_too_large", message: "Use synod file get.", requestID: "r1" } });
});

it("normalizes a fresh protection plan before it reaches a form", async () => {
  const { api } = await import("./api");
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ revision: 1, auto_protect: false, defaults: { destinations: null, method: "auto", at: "03:00", time_zone: "UTC", retention: { daily: 7, weekly: 4, monthly: 6 } }, apps: [] }))));
  expect((await api.protection()).defaults.destinations).toEqual([]);
});

it("pages operations and sends removals and renames to their routes", async () => {
  const { api } = await import("./api");
  const fetch = vi.fn().mockImplementation(() => Promise.resolve(new Response("[]")));
  vi.stubGlobal("fetch", fetch);
  await api.operations({ limit: 51, before: "op 1" });
  expect(fetch.mock.calls[0]?.[0]).toBe("/v1/operations?limit=51&before=op+1");
  await api.operations();
  expect(fetch.mock.calls[1]?.[0]).toBe("/v1/operations");
  fetch.mockImplementation(() => Promise.resolve(new Response(null, { status: 204 })));
  await api.removeOperation("op1");
  expect(fetch.mock.calls[2]?.slice(0, 2)).toEqual(["/v1/operations/op1", expect.objectContaining({ method: "DELETE" })]);
  fetch.mockImplementation(() => Promise.resolve(new Response('{"id":"n1","name":"odin"}')));
  await api.renameNode("n1", "odin");
  expect(fetch.mock.calls[3]?.[1]).toEqual(expect.objectContaining({ method: "PATCH", body: '{"name":"odin"}' }));
});
