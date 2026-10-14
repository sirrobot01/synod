// Join tokens use the format of internal/jointoken: "synod_join_" and raw URL base64 of JSON.

function base64url(bytes: Uint8Array): string {
  let text = "";
  for (const byte of bytes) text += String.fromCharCode(byte);
  return btoa(text).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

/** newSecret returns 32 random bytes in raw URL base64, as the enrollment API requires. */
export function newSecret(): string {
  return base64url(crypto.getRandomValues(new Uint8Array(32)));
}

export interface JoinToken {
  address: string;
  installation_id: string;
  fingerprint: string;
  secret: string;
}

export function encodeJoinToken(token: JoinToken): string {
  return "synod_join_" + base64url(new TextEncoder().encode(JSON.stringify(token)));
}
