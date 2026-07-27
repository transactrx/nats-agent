# Identity, Authority & Delegation — design note

**Status: design note, non-normative. Discussion tabled 2026-07-26.**

This document captures a design discussion about how agents on this protocol
will integrate with the transactrx security model. Nothing here is implemented
yet, and the wire contract in [SPEC.md](SPEC.md) is unchanged. It is written
down so the reasoning is not re-derived later, and so agents built in the
meantime are not built in a way that has to be undone.

The one thing implementers need from this document today is
[§1](#1-what-to-assume-when-building-agents-today).

---

## 1. What to assume when building agents today

1. **Every call will carry a token** (likely a JWT from our own identity
   system) — inbound chat/invoke requests, agent→tool calls, and agent→agent
   calls. Do not build anything that depends on the current unauthenticated
   `userId` field being trustworthy: today it is a claim made by the caller,
   and it will eventually be *derived from a verified token* instead.
2. **Every agent enforces permissions itself, in its deterministic layer** —
   the Go (or Rust/.NET) code around the model, never the model. The model may
   *propose* an action; code decides whether it is allowed. Concretely: the
   authorization check belongs in the tool-dispatch path, not in the system
   prompt.
3. **Never express access control as prompt instructions.** A prompt is not a
   boundary. Anything a user can talk the model into is, by construction, not
   a control.
4. **Assume authority is narrower than the user's.** An agent acts on behalf
   of a user but is itself a principal with its own limited rights (§2). Ask
   for the least you need.

Everything below is the reasoning behind those four rules.

---

## 2. The security model agents plug into

The existing transactrx model, which this design must retain rather than
replace:

- An **app** has an `app_id` and a set of **`app_functions`** — the individual
  rights it exposes.
- A **role** is composed of `app_functions`, freely, **across apps**. An
  administrator can build a role out of rights from several applications; the
  composition is not fixed by the apps.
- A role is granted to a **user within a context** — e.g. the
  `CopayAIAnalyst` role in the context of an `account_id`.
- **Context can be hierarchical**: an `account_id` *and its owned accounts*.
  This is what lets the model express arrangements like an agency holding a
  subset of customers while the agent itself lives with the main company.

That expressiveness is the reason the context has to travel with every call
rather than be resolved once at sign-in.

### 2.1 An agent is an app

The important consequence: **an agent is a principal, not merely code running
as the user.** It has an `app_id` and declares `app_functions`.

So every action has two principals — the user and the agent — and the
effective authority of an action is the **intersection**:

```
effective = (agent's app_functions)
          ∩ (functions the user's roles grant in the ACTIVE context)
```

An agent can never exceed its own registration, and never exceed what the
acting user holds in the context they are currently operating in. Neither
side alone is sufficient.

### 2.2 Enforcement is deterministic, and layered

Authorization lives in the agent's deterministic layer, and **tools enforce
independently** rather than trusting a calling agent to have checked. Two
layers, because an agent bug should not become a data breach.

---

## 3. The gap in the protocol today

`trx.agent.<name>.chat` carries `userId` as an **unauthenticated assertion**
(SPEC §5.1). NATS account permissions control *who may publish* to the agent's
subjects, but nothing establishes *whom the publisher is acting for*. An agent
therefore has no sound basis for an authority decision today.

The fix is a verified token on the request, with `userId` and context derived
from its claims. This is an additive change to the wire contract.

---

## 4. Delegation across the mesh

An agent must not forward the user's raw token to a tool or another agent:
that hands the recipient the user's full authority, defeating §2.1.

Instead, calls carry a **downscoped delegation token**, short-lived, minted
for the triple:

```
(user, acting agent app, active context)  →  intersected function set
```

This is also what an isolated code-execution runner should receive **instead
of credentials** — a delegation that expires and grants only specific
functions, rather than ambient access to databases, secrets, and the mesh.

## 5. Context labels on state

Because one user may act in several contexts, `userId` alone is the wrong key
for anything persistent. Conversation turns and stored artifacts need a
**context label**, and reads must be filtered by the *active* context —
otherwise work produced for one account surfaces while the user is operating
in another. This is the tenancy boundary, and it is where PHI would leak
first.

## 6. Consequences for sessions

Sessions today belong to the agent that created them (SPEC §7: "persistence is
the agent's concern"). The intended direction is that a **session belongs to
the user** and follows them from agent to agent, with each turn attributed to
the agent that produced it.

Under the model in §2 the obvious questions answer themselves:

- *What may a newly joined agent see of the thread?* Whatever the
  role/context/function policy allows at read time. Not a bespoke rule.
- *Participant vs consultant.* A user **switching** to an agent makes it a
  participant (thread context, subject to policy). An agent **consulting**
  another (SPEC roadmap phase 3) passes only the delegated question and
  receives an answer — no transcript handoff. Default-maximal PHI spread is
  otherwise the natural outcome.
- *Transcript portability.* Turns contain `toolUse`/`toolResult` blocks bound
  to a tool set the next agent does not have, and providers require those
  blocks to be well formed against declared tools. A shared session store
  therefore needs a normalized form (foreign tool exchanges collapsed to text
  summaries), with raw blocks retained only for the agent that produced them.

## 7. User storage

The requirement is storage that belongs to the **user**, which an AI accesses
**on behalf of** the user — durable across sessions and across agents.

It should be a **service with its own `app_functions`** (read/write/list),
authorized per call by the delegation token and audited — *not* a filesystem
mounted into an execution environment. A mount grants ambient access to every
user's files; a service can enforce the intersection in §2.1 and label by
context per §5. The underlying storage (EFS, S3) becomes an implementation
detail behind it.

## 8. Autonomous and scheduled execution

The motivating case: *"send me a report every morning."* The user is absent at
execution time, so:

1. Store a **revocable grant** — user U authorizes agent A for functions F in
   context C on this schedule — **never a stored copy of a user token**.
2. **Evaluate authority at execution time, not at scheduling time.** If the
   role was revoked, the account moved between agencies, or the user left, the
   scheduled run must **fail closed**. The grant records intent; the policy
   check must be live.
3. Every autonomous run is **attributable to its grant** for audit, and
   distinguishable from an interactive one.
4. Output lands in the user's storage (§7) under the grant's context.

## 9. Agent card as app registration

If separate teams build and deploy their own agents, two things must hold.

**Registration is the card.** Deploying an agent should declare its `app_id`
and `app_functions` — which is exactly what an administrator needs in order to
compose roles across apps. One artifact, so what an agent can do and what the
security system believes it can do cannot drift.

**Enforcement is turnkey in the library.** `nats-agent` should validate the
inbound token, expose the *effective authority* to agent code, and refuse tool
calls outside the agent's declared functions. Teams should not hand-roll this;
if correct enforcement is optional, it will be optional.

---

## 10. Build order (when resumed)

1. **Delegated identity token** — verified inbound token, derived `userId` +
   context, intersected authority, downscoped delegation for onward calls.
   Everything else depends on it.
2. **Agent card ↔ app registration**, plus library-provided enforcement.
3. **User-owned session/thread service** with context labels and normalized
   transcripts.
4. **User storage service** (§7).
5. **Scheduler with live-evaluated grants** (§8) — needs 1 and 4.
6. **Isolated execution runner**, which becomes straightforward once it can be
   handed a delegation instead of secrets.

## 11. Open questions

- How the identity token is issued and validated (service, keys, rotation),
  and the exact claim shape.
- How **context** is represented in claims, including owned-account
  hierarchies, and whether an agent may switch context within a session.
- Where the token exchange / downscoping lives: a dedicated service, or a
  library-local operation against signing material.
- Grant storage, revocation propagation, and expiry policy for §8.
- Whether tools authorize against the delegation directly, or call a policy
  service.
- Migration for existing per-agent sessions (copayAssistant) into a
  user-owned thread store.
