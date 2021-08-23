import { Observable } from 'rxjs'
import { webSocket, WebSocketSubject, WebSocketSubjectConfig } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  private websocket$: WebSocketSubject<Update<T>> | undefined

  constructor (
    private readonly url: string,
  ) { }

  watch$ (): Observable<Update<T>> {
    const fullConfig: WebSocketSubjectConfig<Update<T>> = {
      url: this.url,
      openObserver: {
        next: () => {
          this.websocket$!.next(document.cookie as any)
        },
      },
    }
    this.websocket$ = webSocket(fullConfig)
    return this.websocket$
  }
}
