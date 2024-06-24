import { Dump, PatchOp } from './types'

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

export type Operation<T> =
  | AddOperation<T>
  | RemoveOperation
  | ReplaceOperation<T>

export function getValueByPointer<T extends Record<string, T>>(
  data: T,
  path: string,
): any {
  if (!path) return data

  try {
    return arrayFromPath(path).reduce((acc, next) => acc[next], data)
  } catch (e) {
    return undefined
  }
}

export function applyOperation<T>(
  doc: Dump<Record<string, any>>,
  { path, op, value }: Operation<T> & { value?: T },
) {
  doc.value = recursiveApply(doc.value, arrayFromPath(path), op, value)
}

export function arrayFromPath(path: string): string[] {
  return path
    .split('/')
    .slice(1)
    .map(p =>
      // order matters, always replace "~1" first
      p.replace(new RegExp('~1', 'g'), '/').replace(new RegExp('~0', 'g'), '~'),
    )
}

export function pathFromArray(args: Array<string | number>): string {
  if (!args.length) return ''

  return (
    '/' +
    args
      .map(a =>
        String(a)
          // do not change order, "~" needs to be replaced first
          .replace(new RegExp('~', 'g'), '~0')
          .replace(new RegExp('/', 'g'), '~1'),
      )
      .join('/')
  )
}

function recursiveApply<T extends Record<string, any> | any[]>(
  data: T,
  path: readonly string[],
  op: PatchOp,
  value?: any,
): T {
  if (!path.length) return value

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

function recursiveApplyObject<T extends Record<string, any>>(
  data: T,
  path: readonly string[],
  op: PatchOp,
  value?: any,
): T {
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

function recursiveApplyArray<T extends any[]>(
  data: T,
  path: readonly string[],
  op: PatchOp,
  value?: any,
): T {
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

function isObject(val: any): val is Record<string, unknown> {
  return typeof val === 'object' && val !== null && !Array.isArray(val)
}
