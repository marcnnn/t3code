import type { ProjectId } from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";

import * as ProjectService from "../project/ProjectService.ts";
import * as ProcessRunner from "../processRunner.ts";

const GUARD_TIMEOUT = Duration.seconds(60);
const GUARD_MAX_OUTPUT_BYTES = 64 * 1024;
const GUARD_REASON_TAIL_CHARS = 500;

export type ScheduledTaskGuardResult =
  | { readonly _tag: "pass"; readonly stdout: string }
  | { readonly _tag: "skip"; readonly reason: string };

export class ScheduledTaskGuard extends Context.Service<
  ScheduledTaskGuard,
  {
    /** Never fails: anything that keeps the guard from saying yes skips the run. */
    readonly run: (input: {
      readonly projectId: ProjectId;
      readonly command: string;
    }) => Effect.Effect<ScheduledTaskGuardResult>;
  }
>()("t3/scheduledTasks/ScheduledTaskGuard") {}

function tail(text: string): string {
  const trimmed = text.trim();
  return trimmed.length <= GUARD_REASON_TAIL_CHARS
    ? trimmed
    : `…${trimmed.slice(-GUARD_REASON_TAIL_CHARS)}`;
}

/** @public Service construction is part of the canonical Effect module API. */
export const make = Effect.gen(function* () {
  const projects = yield* ProjectService.ProjectService;
  const runner = yield* ProcessRunner.ProcessRunner;

  const run: ScheduledTaskGuard["Service"]["run"] = Effect.fn("ScheduledTaskGuard.run")(
    function* (input) {
      const project = yield* projects.getById(input.projectId);
      if (Option.isNone(project)) {
        return { _tag: "skip", reason: "The task's project no longer exists." } as const;
      }
      const platform = yield* HostProcess.Platform;
      const [command, args] =
        platform === "win32"
          ? (["cmd.exe", ["/d", "/s", "/c", input.command]] as const)
          : (["/bin/sh", ["-c", input.command]] as const);
      const output = yield* runner.run({
        command,
        args,
        cwd: project.value.workspaceRoot,
        timeout: GUARD_TIMEOUT,
        maxOutputBytes: GUARD_MAX_OUTPUT_BYTES,
        outputMode: "truncate",
        timeoutBehavior: "timedOutResult",
      });
      if (output.timedOut) {
        return {
          _tag: "skip",
          reason: `The guard did not finish within ${Duration.toSeconds(GUARD_TIMEOUT)} seconds.`,
        } as const;
      }
      if (output.code !== 0) {
        const detail = tail(output.stderr) || tail(output.stdout);
        return {
          _tag: "skip",
          reason: `The guard exited with ${output.code ?? "no exit code"}${detail ? `: ${detail}` : "."}`,
        } as const;
      }
      return { _tag: "pass", stdout: output.stdout } as const;
    },
    (effect) =>
      effect.pipe(
        Effect.catch((error) =>
          Effect.succeed({
            _tag: "skip",
            reason: `The guard could not run: ${error.message}`,
          } as const),
        ),
      ),
  );

  return ScheduledTaskGuard.of({ run });
});

export const layer = Layer.effect(ScheduledTaskGuard, make);
