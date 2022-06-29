import { DBCache, PatchOp } from './types'

export interface BaseOperation {
  path: string
}

export interface AddOperation<T> extends BaseOperation {
  op: PatchOp.ADD
  value: T
}

export interface RemoveOperation extends BaseOperation {
  op: PatchOp.REMOVE
}

export interface ReplaceOperation<T> extends BaseOperation {
  op: PatchOp.REPLACE
  value: T
}

export type Operation<T> = AddOperation<T> | RemoveOperation | ReplaceOperation<T>

export function getValueByPointer<T extends Record<string, T>> (data: T, pointer: string): any {
  if (pointer === '/') return data

  try {
    return jsonPathToKeyArray(pointer).reduce((acc, next) => acc[next], data)
  } catch (e) {
    return undefined
  }
}

export function applyOperation<T> (
  doc: DBCache<Record<string, any>>,
  { path, op, value }: Operation<T> & { value?: T },
): Operation<T> | null {
  const current = getValueByPointer(doc.data, path)
  const remove = { op: PatchOp.REMOVE, path} as const
  const add = { op: PatchOp.ADD, path, value: current} as const
  const replace = { op: PatchOp.REPLACE, path, value: current } as const

  doc.data = recursiveApply(doc.data, jsonPathToKeyArray(path), op, value)

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

function recursiveApply<T extends Record<string, any> | any[]> (data: T, path: readonly string[], op: PatchOp, value?: any): T {
  // object
  if (isObject(data)) {
    return recursiveApplyObject(data, path, op, value)
  // array
  } else if (Array.isArray(data)) {
    return recursiveApplyArray(data, path, op, value)
  } else {
    throw 'unreachable'
  }
}

function recursiveApplyObject<T extends Record<string, any>> (data: T, path: readonly string[], op: PatchOp, value?: any): T {
  if (!path.length) return value

  const updated = recursiveApply(data[path[0]], path.slice(1), op, value)
  const result = {
    ...data,
    [path[0]]: updated,
  }

  if (updated === undefined) {
    delete result[path[0]]
  }

  return result
}

function recursiveApplyArray<T extends any[]> (data: T, path: readonly string[], op: PatchOp, value?: any): T {
  if (!path.length) return value

  const index = parseInt(path[0])

  const updated = recursiveApply(data[index], path.slice(1), op, value)
  const result = [...data] as T
  if (op === PatchOp.ADD) {
    result.splice(index, 0, updated)
  } else if (op === PatchOp.REPLACE) {
    result.splice(index, 1, updated)
  } else {
    result.splice(index, 1)
  }

  return result
}

function isObject (val: any): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null && !Array.isArray(val)
}

function jsonPathToKeyArray (path: string): string[] {
  return path.split('/').slice(1)
}


