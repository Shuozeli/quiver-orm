# Agent-Driven Learning Pipeline

## Problem

Asking an agent "learn this project" produces shallow, unstructured output. The agent
skims a few files, dumps a summary, and forgets everything next session. There is no
accountability, no progress tracking, and no persistent knowledge artifact.

## Solution: Structured Self-Driving Pipeline

Instead of asking the agent to learn directly, create a **closed-loop pipeline** where:

1. The agent drafts a learning plan (structured as goals/milestones/tasks)
2. A tracking tool persists the plan as queryable data
3. The agent drives itself through the plan, reading code and synthesizing lessons
4. Progress is tracked in the tool, creating an auditable trail
5. The accumulated knowledge lives in a database, not just in conversation context

The human's role shifts from "tell me about this code" to "guide me through learning
this" -- but the real effect is the agent systematically teaching itself with the human
as the accountability checkpoint.

## The Pipeline

```
Phase 1: EXPLORE     Agent explores the codebase (structure, tech stack, entry points)
                     Output: raw understanding of project scope
                              |
Phase 2: PLAN        Agent drafts a learning plan as a goal/milestone/task hierarchy
                     Output: structured plan with clear ordering and dependencies
                              |
Phase 3: TOOLING     Build or reuse a progress tracking tool (SQLite-backed CLI)
                     Output: persistent, queryable task database
                              |
Phase 4: EXECUTE     Agent drives through each task:
                       - Marks task in-progress
                       - Reads the relevant code/docs
                       - Synthesizes a distilled lesson for the human
                       - Marks task done
                       - Checks dashboard, picks next task
                     Output: lessons delivered + progress tracked
                              |
Phase 5: KNOWLEDGE   All learnings are captured in conversation history.
                     The task database serves as an index of what was covered.
                     Output: auditable, resumable knowledge base
```

## How to Apply This to Any New Project

### Step 1: Bootstrap prompt
```
I need to learn this project. Draft a plan to explore this codebase and create
a learning plan.
```

### Step 2: Build or reuse a tracker
If a tracker already exists (like `beu`), use it. Otherwise, the agent builds a
minimal one. Requirements:
- Hierarchical: goals > milestones > tasks
- Persistent: SQLite or similar (survives session restarts)
- Queryable: progress dashboard, per-goal drill-down
- CLI-based: agent can drive it programmatically

### Step 3: Load the plan
The agent populates the tracker with the learning plan. This is the "curriculum."

### Step 4: Drive prompt
```
You should guide me how to learn. You decide what I should do next.
```

### Step 5: Repeat across sessions
The tracker persists between sessions. A new agent can check progress and continue.

## Generalizations Beyond Learning

This pipeline works for any large, multi-step agent task:

| Domain              | Goals                        | Tasks                              |
|---------------------|------------------------------|------------------------------------|
| **Learn a project** | Architecture, Data Model,... | Read file X, trace path Y          |
| **Migrate a system**| API layer, DB layer,...      | Migrate endpoint X, update schema  |
| **Audit codebase**  | Security, Performance,...    | Check auth in module X, profile Y  |
| **Onboard new dev** | Setup, Core concepts,...     | Configure env, understand module X |
| **Refactor**        | Extract module, Update API,..| Move functions, update callers     |

## Anti-Patterns

- **No tracker**: Without persistent state, the agent forgets what it covered.
- **Human-driven sequencing**: If the human picks tasks, the agent loses the "must understand enough to teach" forcing function.
- **Too granular**: 200 tasks creates overhead. Aim for 30-50 tasks across 5-7 goals.
- **No lessons**: If the agent just marks tasks done without synthesizing lessons, the human gets no value.
- **Skipping Phase 1**: Without exploration first, the plan will miss critical areas.
