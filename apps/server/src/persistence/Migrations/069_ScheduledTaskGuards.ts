import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN guard_command TEXT`;
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN last_guard_skip_at TEXT`;
  yield* sql`ALTER TABLE scheduled_tasks ADD COLUMN last_guard_skip_reason TEXT`;
});
