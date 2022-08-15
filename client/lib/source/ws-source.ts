import { Observable, timeout } from 'rxjs'
import { webSocket } from 'rxjs/webSocket'
import { Update } from '../types'
import { Source } from './source'

export class WebsocketSource<T> implements Source<T> {
  constructor(private readonly url: string) {}

  watch$(): Observable<Update<T>> {
    return webSocket<Update<T>>(this.url).pipe(timeout({ first: 60000 }))
  }
}
