/** `https://hooks.example.com/…`: an error message never carries a URL's path or query (tokens live there). */
export function redactUrl(url: string): string {
  try {
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.host}${parsed.pathname === '/' && !parsed.search ? '/' : '/…'}`;
  } catch {
    return '(an invalid URL)';
  }
}

const MAX_ERROR_LENGTH = 1_000;

/** A compact, storable description of whatever was thrown. Never throws. */
export function describeError(error: unknown): string {
  let text: string;
  try {
    if (error instanceof Error) {
      const code = (error as { code?: unknown }).code;
      const message = error.message || aggregatedMessage(error);
      text = `${error.name}: ${message}${typeof code === 'string' && !message.includes(code) ? ` (${code})` : ''}`;
    } else if (typeof error === 'string') {
      text = error;
    } else {
      text = JSON.stringify(error) ?? String(error);
    }
  } catch {
    text = Object.prototype.toString.call(error);
  }

  return text.length > MAX_ERROR_LENGTH ? `${text.slice(0, MAX_ERROR_LENGTH)}…` : text;
}

/** An `AggregateError` without a message (node:net's, when every address of a host failed): its errors' messages. */
function aggregatedMessage(error: Error): string {
  return error instanceof AggregateError ? error.errors.map((inner) => (inner instanceof Error ? inner.message : String(inner))).join('; ') : '';
}
