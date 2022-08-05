import { Observable } from 'rxjs'
import { webSocket, WebSocketSubject } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  private websocket$: WebSocketSubject<RPCResponse<Update<T>>> = webSocket({
    url: this.url,
    openObserver: {
      next: () => {
        this.websocket$.next(this.document.cookie as any)
      },
    },
  })

  constructor(
    private readonly url: string,
    private readonly document: Document,
  ) {}

  watch$(): Observable<RPCResponse<Update<T>>> {
    return this.websocket$
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
