import { from, Observable, of, repeatWhen, timer } from 'rxjs'
import { concatMap, map, take } from 'rxjs/operators'
import { Store } from '../store'
import { Http, Update } from '../types'
import { Source } from './source'

export type PollConfig = {
  cooldown: number
}

export class PollSource<T> implements Source<T> {
  constructor(
    private readonly pollConfig: PollConfig,
    private readonly http: Http<T>,
  ) {}

  watch$({ sequence$ }: Store<T>): Observable<Update<T>> {
    return sequence$.pipe(
      concatMap(seq => this.http.getRevisions(seq)),
      take(1),
      // convert Revision[] it into Observable<Revision>. Convert Dump<T> into Observable<Dump<T>>
      concatMap(res => (Array.isArray(res) ? from(res) : of(res))),
      repeatWhen(() => timer(this.pollConfig.cooldown)),
    )
  }
}
