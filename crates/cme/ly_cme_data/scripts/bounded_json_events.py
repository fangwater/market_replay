"""Stream existing JSON arrays without loading an archive or keeping a cache."""
import json

CHUNK = 64 * 1024
MAX_RECORD_CHARS = 1024 * 1024
MAX_GROUP_EVENTS = 250_000
MAX_GROUP_CHARS = 128 * 1024 * 1024


def iter_events(path, chunk_size=CHUNK):
    decoder = json.JSONDecoder()
    with path.open(encoding='utf-8') as stream:
        buffer = ''
        eof = False

        def consume_space():
            nonlocal buffer, eof
            while True:
                buffer = buffer.lstrip()
                if buffer or eof:
                    return
                part = stream.read(chunk_size)
                eof = not part
                buffer += part

        consume_space()
        if not buffer.startswith('['):
            raise ValueError(f'{path}: expected JSON array')
        buffer = buffer[1:]
        consume_space()
        first = True
        while not buffer.startswith(']'):
            if not first:
                if not buffer.startswith(','):
                    raise ValueError(f'{path}: missing event separator or truncated array')
                buffer = buffer[1:]
                consume_space()
            while True:
                try:
                    value, end = decoder.raw_decode(buffer)
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise ValueError(f'{path}: truncated/invalid JSON event') from None
                    if len(buffer) >= MAX_RECORD_CHARS:
                        raise ValueError(f'{path}: event exceeds bounded parser capacity') from None
                    part = stream.read(chunk_size)
                    eof = not part
                    buffer += part
            if not isinstance(value, dict) or end > MAX_RECORD_CHARS:
                raise ValueError(f'{path}: expected a bounded event object')
            buffer = buffer[end:]
            yield value, end
            first = False
            consume_space()
        buffer = buffer[1:]
        consume_space()
        if buffer:
            raise ValueError(f'{path}: trailing data after JSON array')


class EventBudget:
    """Bound downstream list-based state logic until disk-backed processing exists."""
    def __init__(self):
        self.rows = 0
        self.chars = 0

    def account(self, chars):
        self.rows += 1
        self.chars += chars
        if self.rows > MAX_GROUP_EVENTS or self.chars > MAX_GROUP_CHARS:
            raise RuntimeError('status/correction group exceeds bounded in-memory capacity '
                               '(250000 events / 128 Mi characters); disk-backed state processing '
                               'is required before this group can run; no events were discarded')
