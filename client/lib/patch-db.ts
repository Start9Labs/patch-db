import { EMPTY, merge, Observable, ReplaySubject, Subject } from 'rxjs'
import { catchError, switchMap } from 'rxjs/operators'
import { Store } from './store'
import { DBCache, Http } from './types'
import { RPCError } from './source/ws-source'
import { Source } from './source/source'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.http, this.initialCache)
  public connectionError$ = new Subject<Error>()
  public rpcError$ = new Subject<RPCError>()
  public cache$ = new ReplaySubject<DBCache<T>>(1)

  private sub = this.sources$.pipe(
    switchMap(sources => merge(...sources.map(s => s.watch$(this.store))).pipe(
      catchError(e => {
        this.connectionError$.next(e)

        return EMPTY
      }),
    )),
  ).subscribe((res) => {
    if ('result' in res) {
      this.store.update(res.result)
      this.cache$.next(this.store.cache)
    } else {
      this.rpcError$.next(res)
    }
  })

  constructor (
    private readonly sources$: Observable<Source<T>[]>,
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) { }

  clean () {
    this.sub.unsubscribe()
  }
}
