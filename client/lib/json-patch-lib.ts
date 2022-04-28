import { DBCache, PatchOp } from './types'

export interface Validator<T> {
  (operation: Operation, index: number, doc: T, existingPathFragment: string): void
}

export interface BaseOperation {
  path: string
}

export interface AddOperation<T> extends BaseOperation {
  op: PatchOp.ADD
  value: T
}

export interface RemoveOperation extends BaseOperation {
  op: PatchOp.REMOVE
  value?: undefined
}

export interface ReplaceOperation<T> extends BaseOperation {
  op: PatchOp.REPLACE
  value: T
}

export type Operation = AddOperation<any> | RemoveOperation | ReplaceOperation<any>

export function getValueByPointer (doc: Record<string, any>, pointer: string): any {
  if (pointer === '/') return doc

  try {
    return pointer.split('/').slice(1).reduce((acc, next) => acc[next], doc)
  } catch (e) {
    return undefined
  }
}

export function applyOperation (doc: DBCache<Record<string, any>>, { path, op, value }: Operation): Operation | null {
  const current = getValueByPointer(doc.data, path)
  const remove = { op: PatchOp.REMOVE, path} as const
  const add = { op: PatchOp.ADD, path, value: current} as const
  const replace = { op: PatchOp.REPLACE, path, value: current } as const

  doc.data = recursiveApply(doc.data, path.split('/').slice(1), value)

  switch (op) {
    case PatchOp.REMOVE:
      return current === undefined
        ? null
        : add
    case PatchOp.REPLACE:
    case PatchOp.ADD:
      return current === undefined
        ? remove
        : replace
  }
}

function recursiveApply<T extends Record<string, T>> (data: T, path: readonly string[], value?: any): T {
  if (!path.length) return value

  if (!isObject(data)) {
    throw Error('Patch cannot be applied. Path contains non object')
  }

  const updated = recursiveApply(data[path[0]], path.slice(1), value)
  const result = {
    ...data,
    [path[0]]: updated,
  }

  if (updated === undefined) {
    delete result[path[0]]
  }

  return result
}

function isObject (val: any): val is Record<string, any> {
  return typeof val === 'object' && !Array.isArray(val) && val !== null
}


