import { EMPTY, map, merge, Observable, shareReplay, Subject } from 'rxjs'
import { catchError, filter, switchMap, tap } from 'rxjs/operators'
import { Store } from './store'
import { DBCache, Http } from './types'
import { Source } from './source/source'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.http, this.initialCache)
  public connectionError$ = new Subject<any>()
  public cache$ = this.sources$.pipe(
    switchMap(sources =>
      merge(...sources.map(s => s.watch$(this.store))).pipe(
        catchError(e => {
          this.connectionError$.next(e)

          return EMPTY
        }),
      ),
    ),
    tap(_ => this.connectionError$.next(null)),
    filter(Boolean),
    map(res => {
      this.store.update(res)
      return this.store.cache
    }),
    shareReplay(1),
  )

  constructor(
    private readonly sources$: Observable<Source<T>[]>,
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {}
}
