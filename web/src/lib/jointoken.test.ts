import { expect, it } from "vitest";
import { encodeJoinToken, newSecret } from "./jointoken";
it("encodes the token format that agents decode", () => {
  const secret = newSecret();
  expect(secret).toMatch(/^[A-Za-z0-9_-]{43}$/);
  const token = encodeJoinToken({ address: "192.0.2.20:8443", installation_id: "7d0f0f5e-9f5e-4a39-8d3a-2b3a4c5d6e7f", fingerprint: "sha256:" + "ab".repeat(32), secret });
  expect(token.startsWith("synod_join_")).toBe(true);
  const raw = token.slice("synod_join_".length);
  expect(raw).toMatch(/^[A-Za-z0-9_-]+$/);
  const decoded = JSON.parse(atob(raw.replaceAll("-", "+").replaceAll("_", "/")));
  expect(decoded).toEqual({ address: "192.0.2.20:8443", installation_id: "7d0f0f5e-9f5e-4a39-8d3a-2b3a4c5d6e7f", fingerprint: "sha256:" + "ab".repeat(32), secret });
});
