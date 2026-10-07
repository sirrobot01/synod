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
