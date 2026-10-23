import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import { ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";

import * as ProjectService from "../project/ProjectService.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ScheduledTaskGuard from "./ScheduledTaskGuard.ts";

const projectId = ProjectId.make("project-guard");

const layerFor = (workspaceRoot: string | null) =>
  ScheduledTaskGuard.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectService.ProjectService)({
          getById: () =>
            Effect.succeed(
              workspaceRoot === null ? Option.none() : Option.some({ workspaceRoot } as never),
            ),
        }),
        ProcessRunner.layer.pipe(Layer.provide(NodeServices.layer)),
      ),
    ),
  );

const run = (command: string, workspaceRoot: string | null) =>
  ScheduledTaskGuard.ScheduledTaskGuard.pipe(
    Effect.flatMap((guard) => guard.run({ projectId, command })),
    Effect.provide(layerFor(workspaceRoot)),
  );

// Real subprocesses, so wall-clock: the guard's only job is to run a shell.
it.live("passes with stdout when the command exits 0 in the project folder", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.realPath(yield* fs.makeTempDirectoryScoped({ prefix: "guard-" }));
    const result = yield* run(`printf '{"cwd":"%s"}' "$(pwd -P)"`, root);
    assert.deepEqual(result, { _tag: "pass", stdout: `{"cwd":"${root}"}` });
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.live("skips with the exit code and stderr when the command fails", () =>
  Effect.gen(function* () {
    const result = yield* run("echo 'column is empty' >&2; exit 3", "/");
    assert.deepEqual(result, {
      _tag: "skip",
      reason: "The guard exited with 3: column is empty",
    });
  }),
);

it.live("skips when the project is gone", () =>
  Effect.gen(function* () {
    const result = yield* run("true", null);
    assert.deepEqual(result, { _tag: "skip", reason: "The task's project no longer exists." });
  }),
);
