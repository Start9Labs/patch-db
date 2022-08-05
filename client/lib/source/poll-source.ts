import {
  BehaviorSubject,
  concat,
  from,
  ignoreElements,
  Observable,
  of,
  repeatWhen,
  timer,
} from 'rxjs'
import { concatMap, map, switchMap, take, tap } from 'rxjs/operators'
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

  watch$({ sequence$ }: Store<T>): Observable<RPCResponse<Update<T>>> {
    return sequence$.pipe(
      concatMap(seq => this.http.getRevisions(seq)),
      take(1),
      // Revision[] converted it into Observable<Revision>, Dump<T> into Observable<Dump<T>>
      concatMap(res => (Array.isArray(res) ? from(res) : of(res))),
      map(result => ({ result, jsonrpc: '2.0' as const })),
      repeatWhen(() => timer(this.pollConfig.cooldown)),
    )
  }
}
