import { Operation } from './json-patch-lib'

export type Revision = {
  id: number
  patch: Operation<unknown>[]
}

export type Dump<T> = { id: number; value: T }

export type Update<T> = Revision | Dump<T>

export enum PatchOp {
  ADD = 'add',
  REMOVE = 'remove',
  REPLACE = 'replace',
}
