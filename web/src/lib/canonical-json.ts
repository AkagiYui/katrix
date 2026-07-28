// Canonical JSON encoding per the Matrix specification. Used for signing
// device keys, cross-signing keys, and any JSON that must be byte-for-byte
// reproducible across implementations for signature verification.
//
// Rules (spec §Appendix "Canonical JSON"):
//  - Object keys sorted by Unicode code point (UTF-16 code unit) ascending.
//  - No insignificant whitespace.
//  - No duplicate keys.
//  - Integers rendered without a decimal point or exponent.
//  - Floats forbidden (must be integers in practice for Matrix).
//  - UTF-8 preserved; only the escape sequences ", \, and control chars
//    (U+0000–U+001F) are escaped.

/** Sort comparator for canonical-JSON object keys: by UTF-16 code unit. */
function canonicalCompare(a: string, b: string): number {
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    const ca = a.charCodeAt(i);
    const cb = b.charCodeAt(i);
    if (ca !== cb) return ca - cb;
  }
  return a.length - b.length;
}

/**
 * Encode a value as canonical JSON and return the UTF-8 string. Mirrors the
 * reference algorithm used by Synapse / the Matrix spec.
 */
export function canonicalJSONString(value: unknown): string {
  const parts: string[] = [];
  encode(value, parts);
  return parts.join("");
}

/** Encode into the canonical byte stream. */
function encode(value: unknown, out: string[]): void {
  if (value === null || value === undefined) {
    out.push("null");
    return;
  }
  switch (typeof value) {
    case "boolean":
      out.push(value ? "true" : "false");
      return;
    case "number":
      out.push(encodeNumber(value));
      return;
    case "string":
      out.push(encodeString(value));
      return;
    case "object":
      if (Array.isArray(value)) {
        out.push("[");
        for (let i = 0; i < value.length; i++) {
          if (i > 0) out.push(",");
          encode(value[i], out);
        }
        out.push("]");
        return;
      }
      if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
        // Encode byte arrays as base64 (not standard canonical JSON, but the
        // only place we use raw bytes is as a string value elsewhere).
        encode(String.fromCharCode(...new Uint8Array(value.buffer)), out);
        return;
      }
      encodeObject(value as Record<string, unknown>, out);
      return;
  }
}

function encodeObject(obj: Record<string, unknown>, out: string[]): void {
  const keys = Object.keys(obj).sort(canonicalCompare);
  out.push("{");
  let first = true;
  for (const k of keys) {
    if (first) first = false;
    else out.push(",");
    out.push(encodeString(k));
    out.push(":");
    encode(obj[k], out);
  }
  out.push("}");
}

function encodeString(s: string): string {
  let out = '"';
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c === 0x22) out += '\\"';
    else if (c === 0x5c) out += "\\\\";
    else if (c === 0x08) out += "\\b";
    else if (c === 0x09) out += "\\t";
    else if (c === 0x0a) out += "\\n";
    else if (c === 0x0c) out += "\\f";
    else if (c === 0x0d) out += "\\r";
    else if (c < 0x20) out += "\\u" + c.toString(16).padStart(4, "0");
    else out += s[i];
  }
  return out + '"';
}

function encodeNumber(n: number): string {
  if (!Number.isFinite(n)) {
    throw new Error("canonical JSON: non-finite number");
  }
  if (!Number.isInteger(n)) {
    throw new Error("canonical JSON: floats are forbidden");
  }
  return String(n);
}
