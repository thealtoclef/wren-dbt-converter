# Sec-A — JWT Auth (Resource Server)

**Status:** Planned (replaces shipped trust-only backend)
**Owner:** TBD
**Depends on:** none
**Blocks:** Sec-B production exposure (trust-only is dev-only)

---

## 1. Goals

1. **Cryptographically verify** every bearer JWT before the policy engine
   evaluates it. No code path that reads JWT claims without first
   verifying the signature.
2. **One implementation, three transport shapes.** JWKS URL (rotating key
   set), single static key from URL/env/file, symmetric HMAC secret —
   same verifier, same Starlette backend, same error responses.
3. **Async end-to-end.** JWKS fetches do not block the event loop.
4. **Fail-closed.** Invalid token → 401 with RFC 6750 `WWW-Authenticate`.
   Absent token → anonymous (policy decides what anonymous can see).
5. **Stay an OAuth 2.0 Resource Server.** We verify; we never issue
   tokens, never call login endpoints, never handle credentials.
   Translation (opaque → JWT, mTLS → JWT, session → JWT) belongs in a
   reverse proxy in front of us.

## 2. Non-goals

- **API keys, basic auth, session cookies.** A reverse proxy can mint
  JWTs from any of these; from our point of view the wire format is
  always `Authorization: Bearer <jwt>`.
- **Token introspection (RFC 7662).** Round-trip per request to an
  Authorization Server is an AS concern; opaque tokens belong upstream.
- **Token revocation lists / refresh tokens.** Short `exp` is the
  resource-server answer to revocation.
- **mTLS-bound or DPoP/PoP tokens (RFC 8705 / RFC 9449).** Defer until a
  customer asks; terminate at the proxy if needed.
- **Custom callable key resolvers.** Vault/KMS/HSM integrations belong
  to the future Python-overrides hook (Sec-M), not to this module.
- **Trust-only / unverified mode.** Removed entirely. Any operator who
  wants to skip verification sets `enabled: false`, which treats every
  request as anonymous (no token read at all). There is no third state.

## 3. Architecture

### 3.1 Components at a glance

```
HTTP request (Authorization: Bearer <jwt>)
   │
   ▼
┌─────────────────────────────────────────────────────────────┐
│ JWTAuthBackend  (Starlette AuthenticationBackend)           │
│   - extract bearer token                                    │
│   - delegate to Verifier                                    │
│   - on success → JWTUser(JWTPayload(claims)) + scopes       │
│   - on failure → AuthenticationError → 401 + WWW-Authenticate│
└─────────────────────────────────────────────────────────────┘
   │
   ▼
┌─────────────────────────────────────────────────────────────┐
│ Verifier  (one instance, built from config at startup)      │
│   - joserfc jwt.decode(token, keyset, algorithms=[…])       │
│   - JWTClaimsRegistry.validate(claims) for aud/iss/exp/nbf  │
│   - leeway: pre-skew validator's `now` parameter            │
│   - emit OTel counters per outcome                          │
└─────────────────────────────────────────────────────────────┘
   │ uses
   ▼
┌─────────────────────────────────────────────────────────────┐
│ KeyResolver  (interface)                                    │
│   - JWKSResolver       — httpx.AsyncClient + cachetools.TTLCache│
│   - StaticKeyResolver  — fetch-once at startup, cache forever │
│     (subtypes by source: url / env / file)                  │
│   - returns joserfc KeySet                                  │
└─────────────────────────────────────────────────────────────┘
   │
   ▼
joserfc primitives (KeySet, jwt.decode, JWTClaimsRegistry)
```

### 3.2 Library choice

- **`joserfc`** for JWS/JWT/JWK primitives. `authlib.jose` is deprecated
  as of Authlib 1.7 (removed 1.8); joserfc is the maintained successor
  by the same author, and the surface we use is small (decode +
  KeySet + claims registry).
- **`httpx.AsyncClient`** for JWKS HTTP fetches. Already a dependency
  elsewhere in the codebase — no new transport library.
- **`cachetools.TTLCache`** for the JWKS in-process TTL cache. The
  de facto standard for in-process Python TTL caches; trivially
  composable with httpx. Not a third-party JWKS-fetcher wrapper —
  we want the 40-line composition we control, not a 200-line library
  coupled to PyJWT.

### 3.3 What we explicitly do not write

- **Crypto / signature math** — joserfc handles RSA/ECDSA/HMAC.
- **JWS/JWT/JWK parsing** — joserfc handles all RFC 7515–7519 surface.
- **Algorithm allow-listing** — joserfc enforces it from `algorithms=`.
- **Bearer extraction primitive** — Starlette's `AuthenticationBackend`
  is the documented interface; we implement it.

## 4. Config

### 4.1 Final shape

```yaml
security:
  jwt:
    enabled: true                # bool. false → no verification, every
                                 #   request is anonymous, no token read.
    algorithms: [RS256]          # required when enabled. Pinned allow-list;
                                 #   determines symmetric vs asymmetric.
    audience: dbt-graphql        # optional, str | list[str]
    issuer: https://issuer/      # optional
    leeway: 30                   # seconds; tolerance for exp/nbf vs our clock
    required_claims: [exp]       # extra hard-fail list (sub for forced login etc.)
    roles_claim: scope           # JWT claim to read for Starlette scopes;
                                 #   default reads scope/scp/roles in that order

    # Key source — exactly one of:
    jwks_url: https://issuer/.well-known/jwks.json
    jwks_cache_ttl: 3600         # seconds; only meaningful with jwks_url
    # key_url: https://internal/keys/dbt-graphql.pem
    # key_env: JWT_SECRET        # env var name (NOT the secret value)
    # key_file: /etc/dbt-graphql/jwt.pem
```

### 4.2 Pydantic model

```python
class JWTConfig(BaseModel):
    enabled: bool = False
    algorithms: list[str] = []
    audience: str | list[str] | None = None
    issuer: str | None = None
    leeway: int = defaults.JWT_LEEWAY
    required_claims: list[str] = ["exp"]
    roles_claim: str | None = None

    jwks_url: HttpUrl | None = None
    jwks_cache_ttl: int = defaults.JWT_JWKS_CACHE_TTL
    key_url: HttpUrl | None = None
    key_env: str | None = None
    key_file: Path | None = None

    @model_validator(mode="after")
    def _validate(self) -> "JWTConfig":
        if not self.enabled:
            return self
        if not self.algorithms:
            raise ValueError("security.jwt.algorithms is required when enabled")
        sources = [self.jwks_url, self.key_url, self.key_env, self.key_file]
        if sum(s is not None for s in sources) != 1:
            raise ValueError(
                "security.jwt requires exactly one of: "
                "jwks_url, key_url, key_env, key_file"
            )
        return self
```

`SecurityConfig` gains the `jwt` field:

```python
class SecurityConfig(BaseModel):
    policy_path: Path | None = None
    jwt: JWTConfig = JWTConfig()
```

### 4.3 Defaults

```python
# defaults.py additions
JWT_LEEWAY: Final[int] = 30
JWT_JWKS_CACHE_TTL: Final[int] = 3600
```

### 4.4 Why this shape

- `enabled: bool` — binary toggle, matches the rest of the codebase.
  No `mode: enabled | disabled | trust-only` enum: there is no third
  state we want to support.
- Flat under `security.jwt.*` (no inner `key:` group). Matches Hasura,
  Cube, Auth0; mutually-exclusive validation lives in a `model_validator`
  whether the fields are nested or not, and naming carries scope
  (`jwks_cache_ttl`, `key_url`, `key_env`, `key_file`).
- `jwks_url` is first-class because JWKS carries different *behavior*
  (periodic refresh, kid rotation, refetch-on-miss), not just a
  different *location*. The other three (`key_url`, `key_env`, `key_file`)
  share the same behavior — fetch once at startup, cache forever — and
  are interchangeable from the verifier's point of view.
- Format (PEM vs JWK vs raw HMAC bytes) is auto-detected by joserfc's
  `JsonWebKey.import_key()`. The `algorithms` allow-list cross-checks
  it: an `HS256` allow-list with an RSA PEM key fails at startup.

## 5. Verification flow

```python
# api/security.py (sketch)

class JWTAuthBackend(AuthenticationBackend):
    def __init__(self, verifier: Verifier | None) -> None:
        self._verifier = verifier  # None when jwt.enabled = false

    async def authenticate(self, conn: HTTPConnection):
        if self._verifier is None:
            return AuthCredentials([]), JWTUser(JWTPayload({}))

        auth = conn.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return AuthCredentials([]), JWTUser(JWTPayload({}))

        try:
            claims = await self._verifier.verify(auth[len("Bearer ") :])
        except AuthError as e:
            raise AuthenticationError(str(e)) from e

        user = JWTUser(JWTPayload(claims))
        scopes = ["authenticated", *_extract_scopes(claims, self._verifier.roles_claim)]
        return AuthCredentials(scopes), user
```

`Verifier.verify`:

```python
async def verify(self, token: str) -> dict:
    keyset = await self._key_resolver.get()      # cached or fetched
    try:
        decoded = jwt.decode(token, keyset, algorithms=self._algorithms)
    except (BadSignatureError, DecodeError, ...) as e:
        self._metric("invalid_signature").inc()
        raise AuthError("invalid_token") from e

    self._claims_registry.validate(decoded.claims, now=time.time() - self._leeway)
    self._metric("success").inc()
    return decoded.claims
```

### 5.1 Algorithm-confusion defense

Pinning `algorithms=cfg.algorithms` at decode time is the one-line fix
for the canonical alg-confusion CVE (RS256 token presented to an HMAC
verifier, public key used as secret). joserfc rejects any header `alg`
not in the allow-list before signature verification runs.

### 5.2 Leeway

joserfc's `JWTClaimsRegistry.validate` accepts a `now=` parameter but
does not (as of writing) accept a `leeway=` parameter directly. We
pre-skew `now`: pass `time.time() - self._leeway` so a token whose
`exp` is up to `leeway` seconds in the past still validates, and a
token whose `nbf` is up to `leeway` seconds in the future still
validates. Verify against current joserfc API at implementation time;
upstream a PR if a first-class `leeway=` lands.

### 5.3 Scope extraction

```python
def _extract_scopes(claims: dict, custom_claim: str | None) -> list[str]:
    if custom_claim:
        raw = _read_path(claims, custom_claim)
    else:
        raw = claims.get("scope") or claims.get("scp") or claims.get("roles")
    if isinstance(raw, str):
        return raw.split()
    if isinstance(raw, list):
        return [str(s) for s in raw]
    return []
```

Standard `scope` (RFC 8693) is space-delimited string; `scp` is the
Auth0/MS variant; `roles` is the Keycloak/Cognito variant. Configurable
override (`roles_claim: "https://acme.com/roles"`) handles namespaced
custom claims.

## 6. Key resolution

### 6.1 Interface

```python
class KeyResolver(Protocol):
    async def get(self) -> KeySet: ...
```

### 6.2 JWKSResolver

```python
class JWKSResolver:
    def __init__(self, url: str, cache_ttl: int, http: httpx.AsyncClient):
        self._url = url
        self._http = http
        self._cache: TTLCache[str, KeySet] = TTLCache(maxsize=4, ttl=cache_ttl)
        self._last_good: KeySet | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> KeySet:
        if (cached := self._cache.get("set")) is not None:
            return cached
        async with self._lock:
            if (cached := self._cache.get("set")) is not None:
                return cached
            try:
                resp = await self._http.get(self._url, timeout=5.0)
                resp.raise_for_status()
                keyset = KeySet.import_key_set(resp.json())
            except Exception:
                if self._last_good is not None:
                    self._metric("jwks_fetch_failure").inc()
                    return self._last_good          # fail-soft on transient outage
                raise                                 # cold start — no last-good, fail loud
            self._cache["set"] = keyset
            self._last_good = keyset
            return keyset
```

Behaviour matrix:

| Scenario | Outcome |
|---|---|
| Steady state, key in cache | TTL hit, no network |
| TTL expiry, IdP healthy | One refetch, all concurrent callers see the new set (asyncio.Lock coalesces) |
| TTL expiry, IdP transient outage | Return `last_good` keyset, increment failure metric, retry next request |
| Cold start, IdP outage | Raise — there is no last-good. App startup either fails or comes up serving 401. |
| Token presents unknown `kid` | joserfc decode raises; backend returns 401. We do *not* force a JWKS refetch on every kid miss — that's a thundering-herd vector. Operator sets a tighter `jwks_cache_ttl` if rotations are frequent. |

### 6.3 StaticKeyResolver

One class with three constructors. All three fetch once at startup,
cache forever, return the same `KeySet`:

```python
@classmethod
def from_url(cls, url: str, http: httpx.AsyncClient) -> "StaticKeyResolver": ...

@classmethod
def from_env(cls, var: str) -> "StaticKeyResolver":
    raw = os.environ.get(var)
    if raw is None:
        raise ValueError(f"env var {var} not set")
    return cls(_parse_key_material(raw))

@classmethod
def from_file(cls, path: Path) -> "StaticKeyResolver":
    return cls(_parse_key_material(path.read_bytes()))
```

`_parse_key_material` delegates to `JsonWebKey.import_key`, which
auto-detects PEM, JWK JSON, or raw bytes (HMAC). The `algorithms`
allow-list is the cross-check: building the verifier raises if
allow-list and key-type disagree.

## 7. Failure modes and fail-closed semantics

| Wire state | Verifier outcome | Backend response | OTel counter |
|---|---|---|---|
| `enabled: false` | n/a | anonymous user | — |
| No `Authorization` header | n/a | anonymous user | — |
| Header present, malformed (not 3 segments) | `DecodeError` | 401 + `WWW-Authenticate: Bearer error="invalid_token"` | `invalid_signature` |
| Header present, `alg` not in allow-list | `BadSignatureError` (joserfc rejects pre-verify) | 401 | `invalid_signature` |
| Signature invalid | `BadSignatureError` | 401 | `invalid_signature` |
| `exp` expired (past leeway) | `ExpiredTokenError` | 401 + `error="invalid_token", error_description="token expired"` | `expired` |
| `aud` mismatch | `InvalidClaimError` | 401 | `wrong_aud` |
| `iss` mismatch | `InvalidClaimError` | 401 | `wrong_iss` |
| Unknown `kid` | `BadSignatureError` (no key matched) | 401 | `no_kid` |
| JWKS endpoint down, last-good available | success or current outcome | per outcome | `jwks_fetch_failure` (separately) |
| JWKS endpoint down, cold start | startup failure or 503 | per `fail_on_startup` policy | `jwks_fetch_failure` |

Backend → 401 is wired via Starlette `AuthenticationMiddleware(on_error=...)`
returning a `Response` with the RFC 6750 `WWW-Authenticate` header.

## 8. Observability

OTel counter `auth.jwt` with attribute `outcome` ∈
`{success, invalid_signature, expired, wrong_aud, wrong_iss, no_kid,
jwks_fetch_failure}`. One counter, attribute-keyed — same pattern as the
existing `policy.eval` counters.

Per-request span attributes:

- `auth.jwt.kid` (when present)
- `auth.jwt.iss` (when present)
- `auth.jwt.sub` (when present)
- `auth.jwt.outcome`

We **never** log or attribute the raw token, the signature, or any
header beyond `kid` / `alg`.

Clock-skew warning: if more than 1% of tokens validated within `leeway`
seconds of `exp`, log a warning at info level once per minute. Usually
means our clock and the IdP's are drifting.

## 9. File layout

| File | Purpose |
|---|---|
| `src/dbt_graphql/api/auth/__init__.py` | Re-exports `JWTAuthBackend`, `JWTPayload`, `JWTUser` |
| `src/dbt_graphql/api/auth/backend.py` | `JWTAuthBackend`, `JWTUser`, `JWTPayload` (move from current `security.py`) |
| `src/dbt_graphql/api/auth/verifier.py` | `Verifier`, `AuthError`, claims-registry construction, scope extraction |
| `src/dbt_graphql/api/auth/keys.py` | `KeyResolver`, `JWKSResolver`, `StaticKeyResolver` |
| `src/dbt_graphql/api/auth/factory.py` | `build_auth_backend(cfg: JWTConfig) -> JWTAuthBackend` — the only entry point `app.py` calls |
| `src/dbt_graphql/config.py` | `JWTConfig` added to `SecurityConfig` |
| `src/dbt_graphql/defaults.py` | `JWT_LEEWAY`, `JWT_JWKS_CACHE_TTL` |
| `src/dbt_graphql/api/app.py` | Replace direct `JWTAuthBackend()` with `build_auth_backend(cfg.security.jwt)`; wire `on_error=` for RFC 6750 response |
| `tests/unit/api/auth/` | Per-source verifier tests, alg-confusion regression, claims validation, leeway boundary |
| `tests/integration/api/test_jwt_jwks.py` | Real httpx + responses mock for JWKS rotation, fail-soft, cold-start failure |

The current `src/dbt_graphql/api/security.py` becomes a thin
back-compat re-export and is deleted in a follow-up once nothing
imports from it.

## 10. Implementation steps

1. **Add `JWTConfig` to `config.py`** + defaults + `model_validator`.
   Tests for the validator (each illegal combo → `ValidationError`).
2. **Carve `auth/` package out of `security.py`.** Move `JWTPayload`,
   `JWTUser`, `JWTAuthBackend` skeleton. No behavior change yet.
3. **Implement `StaticKeyResolver`** (env/file first — no network).
   Implement `Verifier` against it. Wire via `factory.build_auth_backend`.
   Tests: HMAC happy path, RSA happy path, alg-confusion regression,
   exp boundary with `leeway`, aud/iss mismatch, malformed token.
4. **Implement RFC 6750 error response.** `on_error` handler on
   `AuthenticationMiddleware` returns 401 with `WWW-Authenticate`
   header. Test: 401 + header on every failure mode.
5. **Implement `StaticKeyResolver.from_url`** + httpx async fetch.
   One integration test against a `responses`-mocked URL.
6. **Implement `JWKSResolver`** with TTLCache, asyncio.Lock, last-good
   fallback. Tests: cold-start failure, last-good fallback, kid-rotation
   handled by cache TTL, concurrent callers coalesced via lock.
7. **Scope extraction** + configurable `roles_claim`. Tests: standard
   `scope`, namespaced custom claim, missing claim.
8. **OTel counters + span attributes.** Test: counters increment per
   outcome.
9. **Remove trust-only path.** Delete `verify_signature: False` branch.
   Update README and `config.example.yml`. Update ROADMAP Sec-A row to
   ✅ Done.

Each step lands as its own PR; nothing in the chain depends on a future
step except step 9 (removal), which only lands after all preceding
steps are merged and `enabled: true` is exercised in staging.

## 11. Testing

### Unit (per-mode)

- `key_env` HMAC happy path with `HS256`
- `key_env` HMAC rejected when token alg is `RS256` (alg-confusion)
- `key_file` PEM happy path with `RS256`
- `key_url` static PEM fetched once and cached
- `jwks_url` happy path with `kid` selection across two keys
- `jwks_url` rotation: key A in cache, IdP returns A+B, token signed by B,
  validates after refetch
- `jwks_url` cache TTL: two requests inside TTL → one HTTP fetch
- `jwks_url` IdP outage with last-good → still validates, counter increments
- `jwks_url` cold-start IdP outage → startup fails (configurable)
- `exp` expired exactly at `leeway` boundary: still valid; `leeway + 1` past: invalid
- `aud` mismatch → 401
- `iss` mismatch → 401
- `required_claims: [sub]` and token has no `sub` → 401
- malformed token (not 3 segments) → 401
- `enabled: false` + valid token → anonymous (token not read)
- `enabled: false` + no token → anonymous

### Integration (Starlette + http)

- `test_jwt_jwks.py` — real Starlette client, mock JWKS URL via httpx
  mock transport, drive a full request/response cycle including the
  `WWW-Authenticate` header on failure.

### Regression

- Alg-confusion (the canonical CVE): RS256 token, HMAC verifier,
  public key as the secret → must reject.

## 12. Out of scope (deferred to other ROADMAP entries)

- **Custom callable / programmatic key resolvers** → Sec-M
  (Python-overrides hook). Solved once for keys, mask functions, audit
  sinks, cache backends; not solved per-feature here.
- **Dynamic policy reload on JWT signing-key rotation** → not needed.
  Key rotation only affects verification, which the JWKS cache TTL
  handles transparently.
- **Multiple simultaneous issuers** (multi-tenant JWT). Defer until a
  customer asks; the current `issuer` field is single-valued.

## 13. Decision log

| Decision | Choice | Reasoning |
|---|---|---|
| JOSE library | joserfc | `authlib.jose` deprecated in 1.7. joserfc is the maintained successor. |
| JWKS fetcher | DIY (`httpx` + `cachetools.TTLCache`) | ~40 LOC; `pyjwt-key-fetcher` couples us to PyJWT. |
| Bearer extraction | Starlette `AuthenticationBackend` | Documented interface; no Authlib Starlette integration exists. |
| Async JWKS | httpx async | PyJWT/PyJWK is sync; `to_thread` works but native async is cleaner. |
| Trust-only fallback | Removed | `enabled: false` already covers "skip verification"; no env-gated dev mode. |
| Config root | `security.jwt.*` | Sibling to `security.policy_path`, future `security.allowlist`, `security.audit`. |
| Key sources | `jwks_url` first-class; `key_url` / `key_env` / `key_file` for static | JWKS has rotation behavior; static three are interchangeable. |
| Callable key source | Out of scope (→ Sec-M) | Belongs to a general extension hook, not this feature. |
| Leeway encoding | Pre-skew `now=` passed to claims registry | joserfc lacks first-class `leeway=` today. |
