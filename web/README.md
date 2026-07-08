# twog — web

Front end for the twog **comparative-oncology falsification platform**. We try to
**disprove** hypotheses; nothing is ever auto-promoted — a human **operator** holds the
terminal accept/promote gate.

This is a Next.js 14 (App Router) + TypeScript + Tailwind app with shadcn-style
primitives. Auth is **WorkOS AuthKit** (`@workos-inc/authkit-nextjs`), used headless —
we build our own UI.

## Two surfaces (v1)

- **Contribute** (`/contribute`) — for collaborators: apply, dashboard (profile, scopes,
  custodial/self-held key toggle), open a sandbox (manifest + contribution modes), submit
  contributions (evidence capsule + candidate proposal), and track them through the
  confound + provenance gates to the operator decision.
- **Operate** (`/operate`) — for operators/admins: pending-applicant queue
  (approve/deny + set scopes), collaborators list (revoke), candidate library + detail,
  campaigns list + report (rollup with `any_promoted=false` shown prominently), the
  **write gate** (review submitted capsules → accept/promote), and the verified-target
  catalog.

## Run

```bash
pnpm install            # or npm install
# configure WorkOS AuthKit env (see .env.example) in .env.local
pnpm dev                # http://localhost:3000  (root redirects to /contribute)
```

Other scripts: `pnpm build`, `pnpm start`, `pnpm lint`, `pnpm typecheck`.

## Mock vs real data

A mock/real switch (`NEXT_PUBLIC_USE_MOCKS`) selects mock fixtures vs live fetch. v1 is
wired to **mock** data; there is no live backend yet. Set `NEXT_PUBLIC_USE_MOCKS=false`
only once a real API is available.

## Domain model

Mirrored from the backend contract in [`lib/types/domain.ts`](./lib/types/domain.ts):
`Collaborator`, `SandboxBundle` (+ contribution modes), `Candidate`,
`RunManifest`/`Campaign`, `ProofCapsule`, and the scope/role enums. Do not redesign these
types — they mirror the backend.

## Auth (WorkOS AuthKit, headless)

Auth answers *who* (WorkOS) and authorization answers *what they may do* (twog
scopes). Being signed in is necessary but never sufficient — every privileged
surface also requires a **scope**.

### Required env (`.env.local`)

| Var | Purpose |
| --- | --- |
| `WORKOS_API_KEY` | WorkOS secret API key (server). |
| `WORKOS_CLIENT_ID` | WorkOS client id. |
| `WORKOS_COOKIE_PASSWORD` | 32-byte secret sealing the session cookie (`openssl rand -base64 32`). |
| `WORKOS_REDIRECT_URI` | Callback URI AuthKit reads server-side; must match `/callback`. |
| `NEXT_PUBLIC_WORKOS_REDIRECT_URI` | Client-side copy of the redirect URI (keep in sync). |

Also set the **logout return** in the WorkOS dashboard to the app home (`/`),
since this AuthKit version's `signOut()` takes no return argument.

### Flow

- `middleware.ts` runs `authkitMiddleware` with `middlewareAuth.enabled`; `/`,
  `/login`, `/callback` are public, everything else requires a session.
- `/login` redirects to the hosted AuthKit URL (`getSignInUrl`).
- `/callback` runs `handleAuth` and lands signed-in users on `/contribute`.
- `lib/auth/actions.ts` exports `signInAction` / `signUpAction` / `signOutAction`.

### Principal & scope gating

A WorkOS user is mapped to a twog **Principal** by looking the email up in the
mock collaborator fixtures (`lib/auth/principal.ts`; becomes a backend fetch
later). Only an **active** collaborator's scopes are effective.

- **Server Components / pages:** `await requireScope("accept_capsule")` —
  redirects to sign-in if unauthenticated, to `/forbidden` if the scope is
  missing. `getPrincipal()` / `requirePrincipal()` for non-gating reads.
- **Route Handlers / Server Actions:** wrap with `withScope(scope, handler)` —
  returns 401/403 JSON and injects the resolved `Principal`. Or call
  `assertScope(scope)` directly (throws `ScopeError`).
- **Client components:** `useSession()` exposes `{ principal, isOperator,
  hasScope, isActive }` for scope-aware UI (UX only — the server is
  authoritative). Requires `<SessionProvider/>` (wired into the layout by the
  app shell).

Operator-only scopes (`accept_capsule`, `promote_candidate`) gate the terminal
write gate. Nothing auto-promotes; a human operator always holds the gate.

## Conventions

- Every list/page has **loading**, **empty**, and **error** states.
- Falsification-first voice in copy ("survived", "refuted", "standing",
  "nothing auto-promoted").
- Calm, trustworthy, data-dense-but-legible brand; light + dark.

---

### Backend API

This front end talks to the twog JSON API (`hsa_research.ingestion_bridge.web_api`):
a WorkOS session token resolves to a `principal`, and `dispatch()` routes to the
service with scope enforcement. Endpoints: `/me`, `/collaborators[/apply|/:id/approve|/revoke]`,
`/candidates[/:id]`, `/campaigns[/:id]`, `/target-library`, `/sandbox/open`,
`/contributions[/mine]`, `/capsules[/:id/accept|/promote]`, `/proposals[/:id/decide]`.
Flip `NEXT_PUBLIC_USE_MOCKS=false` + point `NEXT_PUBLIC_API_BASE_URL` at it to go live.
