import { Operation } from './json-patch-lib'

// revise a collection of nodes.
export type Revision = {
  id: number
  patch: Operation<unknown>[]
}

// dump/replace the entire store with T
export type Dump<T> = { id: number; value: T }

export type Update<T> = Revision | Dump<T>

export enum PatchOp {
  ADD = 'add',
  REMOVE = 'remove',
  REPLACE = 'replace',
}

export interface Bootstrapper<T> {
  init(): DBCache<T>
  update(cache: DBCache<T>): void
}

export interface DBCache<T extends Record<string, any>> {
  sequence: number
  data: T
}
