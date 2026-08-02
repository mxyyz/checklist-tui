#!/usr/bin/env python3
"""A deliberately unreliable proxy, for the kill-mid-sync validation.

Forwards a request to the real relay, waits for the relay to answer - so the
work really is applied on the far side - and then drops the connection without
returning that answer. The client therefore cannot advance its watermark and
must re-send the same batch on its next attempt.

That is the interesting failure: not "nothing happened", but "it happened and
the client does not know". Recovery depends entirely on the merge being
idempotent.

Usage: lossy-proxy.py <listen-port> <upstream-base-url> <drop-count>
"""
import socket
import ssl
import sys
import threading
import urllib.parse

listen_port = int(sys.argv[1])
upstream = urllib.parse.urlparse(sys.argv[2])
drops_remaining = int(sys.argv[3])
lock = threading.Lock()

upstream_host = upstream.hostname
upstream_port = upstream.port or (443 if upstream.scheme == "https" else 80)
use_tls = upstream.scheme == "https"


def handle(client):
    global drops_remaining
    with lock:
        drop = drops_remaining > 0
        if drop:
            drops_remaining -= 1

    try:
        request = b""
        client.settimeout(10)
        # Read headers, then the body if one was announced.
        while b"\r\n\r\n" not in request:
            chunk = client.recv(65536)
            if not chunk:
                return
            request += chunk
        head, _, rest = request.partition(b"\r\n\r\n")
        length = 0
        for line in head.split(b"\r\n"):
            if line.lower().startswith(b"content-length:"):
                length = int(line.split(b":")[1])
        while len(rest) < length:
            chunk = client.recv(65536)
            if not chunk:
                break
            rest += chunk

        # Ask upstream to close when done, so reading to EOF returns promptly
        # instead of stalling on a keep-alive connection until a timeout.
        head = b"\r\n".join(
            l for l in head.split(b"\r\n") if not l.lower().startswith(b"connection:")
        ) + b"\r\nConnection: close"

        raw = socket.create_connection((upstream_host, upstream_port), timeout=15)
        server = (
            ssl.create_default_context().wrap_socket(raw, server_hostname=upstream_host)
            if use_tls
            else raw
        )
        server.sendall(head + b"\r\n\r\n" + rest)

        response = b""
        server.settimeout(15)
        try:
            while True:
                chunk = server.recv(65536)
                if not chunk:
                    break
                response += chunk
        except (TimeoutError, OSError):
            pass
        server.close()

        if drop:
            # Upstream has committed. Say nothing and hang up.
            client.close()
            return
        client.sendall(response)
    except Exception:
        pass
    finally:
        try:
            client.close()
        except OSError:
            pass


listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", listen_port))
listener.listen(8)
print(f"lossy proxy on 127.0.0.1:{listen_port} -> {sys.argv[2]}, dropping {drops_remaining}", flush=True)

while True:
    conn, _ = listener.accept()
    threading.Thread(target=handle, args=(conn,), daemon=True).start()
