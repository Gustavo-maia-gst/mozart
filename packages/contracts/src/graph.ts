import type { TaskId } from './ids';

/** Identity of a DAG. A run may execute several concurrent graphs. */
export type GraphId = string;

export interface GraphTask {
  id: TaskId;
  dependsOn: TaskId[];
  /** Nominal cost (ms); used by W as the task duration when set. */
  costMs?: number;
}

/** A DAG of tasks. Task ids are unique within (and, by convention, across) graphs. */
export interface Graph {
  id: GraphId;
  tasks: GraphTask[];
}
