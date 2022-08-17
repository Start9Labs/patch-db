import { Operation } from './json-patch-lib'

// revise a collection of nodes.
export type Revision = {
  id: number
  patch: Operation<unknown>[]
  expireId: string | null
}

// dump/replace the entire store with T
export type Dump<T> = { id: number; value: T; expireId: string | null }

export type Update<T> = Revision | Dump<T>

export enum PatchOp {
  ADD = 'add',
  REMOVE = 'remove',
  REPLACE = 'replace',
}

export interface Bootstrapper<T> {
  init(): Promise<DBCache<T>>
  update(cache: DBCache<T>): Promise<void>
}

export interface DBCache<T extends Record<string, any>> {
  sequence: number
  data: T
}
