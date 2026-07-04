import { z } from 'zod';

/**
 * Pure-JSON value. Everything that crosses the IPC boundary or is persisted
 * (message bodies, TaskState, event payloads) must be `Json` — no Date, Map,
 * BigInt, undefined or NaN. The IPC channel uses JSON serialization, so
 * anything else would be silently mangled.
 */
export type Json = string | number | boolean | null | Json[] | { [key: string]: Json };

export type JsonObject = { [key: string]: Json };

export const jsonSchema: z.ZodType<Json> = z.lazy(() =>
  z.union([
    z.string(),
    z.number().finite(),
    z.boolean(),
    z.null(),
    z.array(jsonSchema),
    z.record(z.string(), jsonSchema),
  ]),
);

export const jsonObjectSchema: z.ZodType<JsonObject> = z.record(z.string(), jsonSchema);
