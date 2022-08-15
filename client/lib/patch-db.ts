import { EMPTY, merge, Observable, ReplaySubject, Subject } from 'rxjs'
import { catchError, switchMap } from 'rxjs/operators'
import { Store } from './store'
import { DBCache, Http } from './types'
import { Source } from './source/source'

export class PatchDB<T> {
  public store: Store<T> = new Store(this.http, this.initialCache)
  public connectionError$ = new Subject<any>()
  public cache$ = new ReplaySubject<DBCache<T>>(1)

  constructor(
    private readonly sources$: Observable<Source<T>[]>,
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {}

  ngOnInit() {
    this.sources$
      .pipe(
        switchMap(sources =>
          merge(...sources.map(s => s.watch$(this.store))).pipe(
            catchError(e => {
              this.connectionError$.next(e)

              return EMPTY
            }),
          ),
        ),
      )
      .subscribe(res => {
        this.store.update(res)
        this.cache$.next(this.store.cache)
      })
  }
}
