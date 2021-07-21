import { merge, Observable, of } from 'rxjs'
import { concatMap, finalize, tap } from 'rxjs/operators'
import { Source } from './source/source'
import { Store } from './store'
import { DBCache, HashMap, Http } from './types'

export class PatchDB<T extends HashMap> {
  store: Store<T>

  constructor (
    private readonly sources: Source<T>[],
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {
    this.store = new Store(this.http, this.initialCache)
  }

  sync$ (): Observable<DBCache<T>> {
    return merge(...this.sources.map(s => s.watch$(this.store)))
    .pipe(
      tap(update => this.store.update(update)),
      concatMap(() => of(this.store.cache)),
      finalize(() => {
        this.store.reset()
      }),
    )
  }
}
