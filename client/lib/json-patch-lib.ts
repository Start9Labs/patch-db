export interface Validator<T> {
  (operation: Operation, index: number, doc: T, existingPathFragment: string): void
}

export interface BaseOperation {
  path: string
}

export interface AddOperation<T> extends BaseOperation {
  op: 'add'
  value: T
}

export interface RemoveOperation extends BaseOperation {
  op: 'remove'
}

export interface ReplaceOperation<T> extends BaseOperation {
  op: 'replace'
  value: T
}

export type Doc = { [key: string]: any }

export type Operation = AddOperation<any> | RemoveOperation | ReplaceOperation<any>

export function getValueByPointer (doc: any, pointer: string): any {
  if (pointer === '/') return doc
  const pathArr = pointer.split('/')
  pathArr.shift()
  try {
    return pathArr.reduce((acc, next) => acc[next], doc)
  } catch (e) {
    return undefined
  }
}

export function applyOperation (doc: Doc, op: Operation): Operation | null {
  let undo: Operation | null = null
  const pathArr = op.path.split('/')
  pathArr.shift()
  pathArr.reduce((node, key, i) => {
    if (!isObject) {
      throw Error('patch cannot be applied.  Path contains non object')
    }

    if (i < pathArr.length - 1) {
      // iterate node
      return node[key]
    }

    // if last key
    const curVal = node[key]
    if (op.op === 'add' || op.op === 'replace') {
      node[key] = op.value
      if (curVal) {
        undo = {
          op: 'replace',
          path: op.path,
          value: curVal,
        }
      } else {
        undo = {
          op: 'remove',
          path: op.path,
        }
      }
    } else {
      delete node[key]
      if (curVal) {
        undo = {
          op: 'add',
          path: op.path,
          value: curVal,
        }
      }
    }
  }, doc)

  return undo
}

function isObject (val: any): val is Doc {
  return typeof val === 'object' && !Array.isArray(val) && val !== null
}


