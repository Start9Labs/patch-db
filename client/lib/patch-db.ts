import { merge, Observable, Subject, Subscription } from 'rxjs'
import { Store } from './store'
import { DBCache, Http } from './types'
import { RPCError } from './source/ws-source'
import { Source } from './source/source'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.http, this.initialCache)
  public connectionError$ = new Subject<Error>()
  public rpcError$ = new Subject<RPCError>()
  public cache$ = new Subject<DBCache<T>>()

  private updatesSub?: Subscription
  private sourcesSub = this.sources$.subscribe(sources => {
    this.updatesSub = merge(...sources.map(s => s.watch$(this.store))).subscribe({
      next: (res) => {
        if ('result' in res) {
          this.store.update(res.result)
          this.cache$.next(this.store.cache)
        }
        else {
          this.rpcError$.next(res)
        }
      },
      error: (e) => {
        this.connectionError$.next(e)
      },
    })
  })

  constructor (
    private readonly sources$: Observable<Source<T>[]>,
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) { }

  clean () {
    this.sourcesSub.unsubscribe()
    this.updatesSub?.unsubscribe()
  }
}
