import { BehaviorSubject, concat, from, Observable, of } from 'rxjs'
import {
  concatMap,
  delay,
  map,
  skip,
  switchMap,
  take,
  tap,
} from 'rxjs/operators'
import { Store } from '../store'
import { Http, Update } from '../types'
import { Source } from './source'
import { RPCResponse } from './ws-source'

export type PollConfig = {
  cooldown: number
}

export class PollSource<T> implements Source<T> {
  constructor(
    private readonly pollConfig: PollConfig,
    private readonly http: Http<T>,
  ) {}

  watch$(store: Store<T>): Observable<RPCResponse<Update<T>>> {
    const polling$ = new BehaviorSubject('')

    const updates$ = of({}).pipe(
      concatMap(_ => store.sequence$),
      concatMap(seq => this.http.getRevisions(seq)),
      take(1),
    )

    const delay$ = of([]).pipe(
      delay(this.pollConfig.cooldown),
      tap(_ => polling$.next('')),
      skip(1),
    )

    const poll$ = concat(updates$, delay$)

    return polling$.pipe(
      switchMap(_ => poll$),
      concatMap(res => {
        if (Array.isArray(res)) {
          return from(res) // takes Revision[] and converts it into Observable<Revision>
        } else {
          return of(res) // takes Dump<T> and converts it into Observable<Dump<T>>
        }
      }),
      map(result => ({ result, jsonrpc: '2.0' })),
    )
  }
}
