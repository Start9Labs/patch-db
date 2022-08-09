import { Observable, timeout } from 'rxjs'
import { webSocket, WebSocketSubject } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  constructor(
    private readonly url: string,
    // TODO: Remove fallback after client app is updated
    private readonly document: Document = document,
  ) {}

  watch$(): Observable<RPCResponse<Update<T>>> {
    const stream$: WebSocketSubject<RPCResponse<Update<T>>> = webSocket({
      url: this.url,
      openObserver: {
        next: () => stream$.next(this.document.cookie as any),
      },
    })

    return stream$.pipe(timeout(60000))
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

class RpcError {
  code: number
  message: string
  details: string

  constructor(e: RPCError['error']) {
    this.code = e.code
    this.message = e.message
    this.details = e.data.details
  }
}
