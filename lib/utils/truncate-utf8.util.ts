const encoder = new TextEncoder();

/** The longest prefix of `text` that is at most `maxBytes` in UTF-8, never splitting a character. */
export function truncateUtf8(text: string, maxBytes: number): string {
  if (text.length * 3 <= maxBytes) {
    return text;
  }
  const { read } = encoder.encodeInto(text, new Uint8Array(maxBytes));
  return text.slice(0, read);
}
