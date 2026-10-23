import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import { ScheduledTaskUpsertInput } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ScheduledTaskGuard from "./ScheduledTaskGuard.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";

const decodeInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);
const START = Date.parse("2026-10-23T22:00:00.000Z");
const input = {
  title: "Night queue",
  prompt: "/try-board-issue {{guard.number}}",
  enabled: true,
  schedule: { type: "interval", everyMs: 60_000 },
  projectId: "project-guard",
  workspaceStrategy: { type: "root" },
  modelSelection: { instanceId: "claudeAgent", model: "claude-opus-5-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  guardCommand: "./board.py pick",
};

/**
 * Builds the service with a guard that answers `verdict`, records every
 * launched prompt and every guard call, and hands back the scheduler's
 * sources so a test fires one poll by hand instead of advancing a timer.
 */
const harness = Effect.fn(function* (verdict: ScheduledTaskGuard.ScheduledTaskGuardResult) {
  const launched = yield* Ref.make<ReadonlyArray<string>>([]);
  const guardCalls = yield* Ref.make<ReadonlyArray<string>>([]);
  const dueWork = yield* Ref.make<ReadonlyMap<string, Effect.Effect<void>>>(new Map());
  const layer = ScheduledTaskService.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        NodeCrypto.layer,
        Layer.succeed(Scheduler.Scheduler, {
          register: <E, R>(name: string, work: Effect.Effect<void, E, R>) =>
            Effect.gen(function* () {
              const context = yield* Effect.context<R>();
              const run = work.pipe(Effect.provideContext(context), Effect.orDie);
              yield* Ref.update(dueWork, (sources) => new Map(sources).set(name, run));
            }),
        }),
        Layer.mock(ThreadLaunchService.ThreadLaunchService)({
          launch: (launch) =>
            Ref.update(launched, (texts) => [...texts, launch.initialMessage?.text ?? ""]).pipe(
              Effect.as({ threadId: "thread-guard", resumed: false } as never),
            ),
        }),
        Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
          namedProjectsRoot: "/projects",
          isScratchProject: () => Effect.succeed(false),
        }),
        Layer.mock(ThreadManagementService.ThreadManagementService)({}),
        Layer.mock(SecretRequests.SecretRequests)({}),
        Layer.succeed(ScheduledTaskGuard.ScheduledTaskGuard, {
          run: ({ command }) =>
            Ref.update(guardCalls, (calls) => [...calls, command]).pipe(Effect.as(verdict)),
        }),
      ),
    ),
  );
  const poll = Ref.get(dueWork).pipe(
    Effect.flatMap((sources) => Effect.forEach(sources.values(), (run) => run, { discard: true })),
  );
  return { layer, launched, guardCalls, poll };
});

it.effect("a guard that says no skips the run, records why and asks again next time", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START);
    const { layer, launched, guardCalls, poll } = yield* harness({
      _tag: "skip",
      reason: "The guard exited with 1: nothing to do",
    });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const { task } = yield* service.upsert(yield* decodeInput(input));
      yield* TestClock.adjust("61 seconds");
      yield* poll;

      const [skipped] = (yield* service.list()).tasks;
      assert.deepEqual(yield* Ref.get(guardCalls), ["./board.py pick"]);
      assert.deepEqual(yield* Ref.get(launched), []);
      assert.equal(skipped?.lastRunStatus, "never");
      assert.equal(skipped?.runCount, 0);
      assert.equal(skipped?.lastGuardSkip?.reason, "The guard exited with 1: nothing to do");
      assert.equal(skipped?.nextRunAt, DateTime.formatIso(DateTime.makeUnsafe(START + 121_000)));
      assert.notEqual(skipped?.nextRunAt, task.nextRunAt);
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a guard that says yes hands its JSON output to the prompt", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START);
    const { layer, launched, poll } = yield* harness({
      _tag: "pass",
      stdout: '{"number":1737,"title":"MCP OAuth"}\n',
    });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      yield* service.upsert(yield* decodeInput(input));
      yield* TestClock.adjust("61 seconds");
      yield* poll;

      assert.deepEqual(yield* Ref.get(launched), ["/try-board-issue 1737"]);
      const [ran] = (yield* service.list()).tasks;
      assert.equal(ran?.lastRunStatus, "succeeded");
      assert.equal(ran?.runCount, 1);
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("output missing a placeholder's field skips the run instead of sending a gap", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START);
    const { layer, launched, poll } = yield* harness({ _tag: "pass", stdout: "{}" });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      yield* service.upsert(yield* decodeInput(input));
      yield* TestClock.adjust("61 seconds");
      yield* poll;

      assert.deepEqual(yield* Ref.get(launched), []);
      const [skipped] = (yield* service.list()).tasks;
      assert.equal(
        skipped?.lastGuardSkip?.reason,
        "The guard printed no value for {{guard.number}}.",
      );
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("Run now does not ask the guard", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(START);
    const { layer, launched, guardCalls } = yield* harness({ _tag: "skip", reason: "no" });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const { task } = yield* service.upsert(
        yield* decodeInput({ ...input, prompt: "/try-board-issue now" }),
      );
      yield* service.runNow({ id: task.id });

      assert.deepEqual(yield* Ref.get(guardCalls), []);
      assert.deepEqual(yield* Ref.get(launched), ["/try-board-issue now"]);
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("saves keep the guard unless it is replaced or cleared", () =>
  Effect.gen(function* () {
    const { layer } = yield* harness({ _tag: "skip", reason: "no" });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const { guardCommand: _omitted, ...withoutGuard } = input;
      const { task } = yield* service.upsert(yield* decodeInput(input));

      const kept = yield* service.upsert(
        yield* decodeInput({ ...withoutGuard, id: task.id, title: "Renamed" }),
      );
      assert.equal(kept.task.guardCommand, "./board.py pick");

      const cleared = yield* service.upsert(
        yield* decodeInput({ ...withoutGuard, id: task.id, guardCommand: null }),
      );
      assert.isUndefined(cleared.task.guardCommand);
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("a guard needs a timed, full-access task", () =>
  Effect.gen(function* () {
    const { layer } = yield* harness({ _tag: "skip", reason: "no" });
    yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const approval = yield* Effect.exit(
        service.upsert(yield* decodeInput({ ...input, runtimeMode: "approval-required" })),
      );
      assert.isTrue(Exit.isFailure(approval));
      const webhook = yield* Effect.exit(
        service.upsert(
          yield* decodeInput({ ...input, schedule: { type: "webhook", signature: null } }),
        ),
      );
      assert.isTrue(Exit.isFailure(webhook));
      assert.deepEqual((yield* service.list()).tasks, []);
    }).pipe(Effect.provide(layer));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
