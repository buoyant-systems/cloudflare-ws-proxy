# Architecture

Detailed diagrams for cloudflare-ws-proxy.

## Request Flow

```mermaid
sequenceDiagram
    participant Backend as Backend (Cloud Run)
    participant Worker as CF Worker
    participant DO as Durable Object (ProxyDO)
    participant Client as Browser Client

    note over Backend,Worker: Backend requests use Authorization: Bearer secret

    rect rgb(40, 40, 60)
    note right of Backend: 1. Generate auth URL
    Backend->>Worker: POST /topic/:id/auth
    Worker->>Worker: Validate secret, sign HMAC token
    Worker-->>Backend: { url: "wss://…/connect?token=…" }
    end

    rect rgb(40, 60, 40)
    note right of Client: 2. Client connects
    Client->>Worker: GET /topic/:id/connect?token=… (Upgrade: websocket)
    Worker->>Worker: Verify HMAC token
    Worker->>DO: Forward upgrade request
    DO->>DO: acceptWebSocket() → hibernation mode
    DO-->>Client: Replay buffered messages
    note over DO,Client: WebSocket held open at zero cost during hibernation
    end

    rect rgb(60, 40, 40)
    note right of Backend: 3. Publish a message
    Backend->>Worker: POST /topic/:id/publish { message }
    Worker->>DO: Forward message
    DO->>DO: Buffer in storage, set TTL alarm
    DO-->>Client: ws.send(message) → broadcast to all
    note right of Backend: Backend HTTP connection closes immediately
    end

    rect rgb(60, 60, 40)
    note right of Backend: 4. Teardown (optional)
    Backend->>Worker: DELETE /topic/:id
    Worker->>DO: Force teardown
    DO->>DO: Close all sockets, deleteAll()
    end
```

## Component Diagram

```mermaid
flowchart LR
    subgraph "Cloudflare Edge"
        W["Worker\n(src/index.ts)"]
        A["Auth Module\n(src/auth.ts)"]

        subgraph "Per-Topic Durable Object"
            DO["ProxyDO\n(src/proxy-do.ts)"]
            S[("SQLite Storage\n(message buffer)")]
            AL["Alarm\n(TTL cleanup)"]
        end

        W -- "HMAC sign/verify" --> A
        W -- "route to DO" --> DO
        DO -- "read/write" --> S
        DO -- "schedule" --> AL
        AL -- "prune expired" --> S
    end

    BE["Backend\n(Cloud Run / Lambda)"] -- "HTTP POST/DELETE" --> W
    BR["Browser Clients"] <-- "WebSocket\n(hibernated)" --> DO
```

## Durable Object Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Idle: First request (idFromName)
    Idle --> Active: Message published / client connects
    Active --> Hibernating: No events for ~10s
    Hibernating --> Active: Message arrives / alarm fires
    Active --> Idle: All sockets closed + storage empty
    Idle --> [*]: Evicted from memory (zero cost)

    Active --> Destroyed: DELETE /topic/:id
    Hibernating --> Active: Alarm fires (TTL cleanup)
    Active --> Destroyed: Alarm fires (all messages expired)
    Destroyed --> [*]: Storage wiped, sockets closed
```

> **Topic lifecycle:** Each topic's TTL, buffer size, and generation UUID are set once on creation (first publish) and immutable for the topic's lifetime. When a topic is torn down — either by explicit `DELETE` or when all messages expire via TTL — its storage is fully wiped and connections are closed. The next publish creates a new lifecycle with a fresh generation.
