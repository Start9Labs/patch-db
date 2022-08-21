import { map, Observable, shareReplay } from 'rxjs'
import { tap } from 'rxjs/operators'
import { Store } from './store'
import { DBCache, Update } from './types'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.initialCache)
  public cache$ = this.source$.pipe(
    tap(res => this.store.update(res)),
    map(_ => this.store.cache),
    shareReplay(1),
  )

  constructor(
    private readonly source$: Observable<Update<T>>,
    private readonly initialCache: DBCache<T>,
  ) {}
}
