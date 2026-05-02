/**
 * Scripts: SQL invocation layer for {@link Queue}, {@link Worker}, and
 * {@link Job}.
 *
 * Implementation is split across a single linear inheritance chain so that
 * each file stays focused and small enough to read top-to-bottom:
 *
 *   ScriptsBase            — shared context, schema escaping, error mapping
 *     ↳ AddJobsScripts     — `addJob`, `addParentJobForFlow`, payload encoding
 *       ↳ LifecycleScripts — claim/finish/retry/promote/stalled, lock leases
 *         ↳ GettersScripts — counts, ranges, dependency lookups, pagination
 *           ↳ SchedulerScripts — cron / repeat scheduler bookkeeping
 *             ↳ MaintenanceScripts — pause, drain, clean, remove, obliterate
 *               ↳ Scripts — final facade (no methods of its own)
 *
 * Callers should only reference `Scripts`; the intermediate classes are an
 * organisational tool, not a public extension surface.
 */
import { MaintenanceScripts } from './maintenance';

export { MoveToFinishedParams, JobData, raw2NextJobData } from './helpers';

export class Scripts extends MaintenanceScripts {}
