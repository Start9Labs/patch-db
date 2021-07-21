import { Operation } from './json-patch-lib'

// revise a collection of nodes.
export type Revision = { id: number, patch: Operation[], expireId: string | null }
// dump/replace the entire store with T
export type Dump<T extends HashMap> = { id: number, value: T, expireId: string | null }

export type Update<T extends HashMap> = Revision | Dump<T>

export enum PatchOp {
  ADD = 'add',
  REMOVE = 'remove',
  REPLACE = 'replace',
}

export interface Http<T extends HashMap> {
  getRevisions (since: number): Promise<Revision[] | Dump<T>>
  getDump (): Promise<Dump<T>>
}

export interface Bootstrapper<T extends HashMap> {
  init (): Promise<DBCache<T>>
  update (cache: DBCache<T>): Promise<void>
}

export interface DBCache<T extends HashMap>{
  sequence: number,
  data: T
}

export type HashMap = { [type: string]: any }

