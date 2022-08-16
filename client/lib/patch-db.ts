import { map, shareReplay, Subject, timer } from 'rxjs'
import { catchError, concatMap, tap } from 'rxjs/operators'
import { Store } from './store'
import { DBCache, Http } from './types'
import { Source } from './source/source'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.http, this.initialCache)
  public connectionError$ = new Subject<any>()
  public cache$ = this.source.watch$(this.store).pipe(
    catchError((e, watch$) => {
      this.connectionError$.next(e)
      return timer(4000).pipe(concatMap(() => watch$))
    }),
    tap(res => {
      this.connectionError$.next(null)
      this.store.update(res)
    }),
    map(_ => this.store.cache),
    shareReplay(1),
  )

  constructor(
    private readonly source: Source<T>,
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {}
}
