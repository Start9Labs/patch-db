import { from, Observable, of, repeat } from 'rxjs'
import { concatMap, take } from 'rxjs/operators'
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

  watch$({ sequence$ }: Store<T>): Observable<Update<T> | null> {
    return sequence$.pipe(
      concatMap(seq => this.http.getRevisions(seq)),
      take(1),
      concatMap(res => {
        // If Revision[]
        if (Array.isArray(res)) {
          // Convert Revision[] it into Observable<Revision> OR return null
          return res.length ? from(res) : of(null)
          // If Dump<T>
        }
        // Convert Dump<T> into Observable<Dump<T>>
        return of(res)
      }),
      repeat({ delay: this.pollConfig.cooldown }),
    )
  }
}
