import { Observable } from 'rxjs'
import { map } from 'rxjs/operators'
import { webSocket, WebSocketSubject, WebSocketSubjectConfig } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  private websocket$: WebSocketSubject<RPCResponse<Update<T>>> | undefined

  constructor (
    private readonly url: string,
  ) { }

  watch$ (): Observable<Update<T>> {
    const fullConfig: WebSocketSubjectConfig<RPCResponse<Update<T>>> = {
      url: this.url,
      openObserver: {
        next: () => {
          this.websocket$!.next(document.cookie as any)
        },
      },
    }
    this.websocket$ = webSocket(fullConfig)
    return this.websocket$.pipe(
      map(res => {
        if (isRpcSuccess(res)) return res.result
        if (isRpcError(res)) throw new RpcError(res.error)
      }),
    ) as Observable<Update<T>>
  }
}

interface RPCBase {
  jsonrpc: '2.0'
}

export interface RPCSuccess<T> extends RPCBase {
  result: T
}

export interface RPCError extends RPCBase {
  error: {
    code: number // 34 means unauthenticated
    message: string
    data: {
      details: string
    }
  }
}

export type RPCResponse<T> = RPCSuccess<T> | RPCError

function isRpcError<Error, Result> (arg: { error: Error } | { result: Result}): arg is { error: Error } {
  return !!(arg as any).error
}

function isRpcSuccess<Error, Result> (arg: { error: Error } | { result: Result}): arg is { result: Result } {
  return !!(arg as any).result
}

class RpcError {
  code: number
  message: string
  details: string

  constructor (e: RPCError['error']) {
    this.code = e.code
    this.message = e.message
    this.details = e.data.details
  }
}