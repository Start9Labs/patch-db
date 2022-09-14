import { Bootstrapper, DBCache, Dump, Revision, Update } from './types'
import { BehaviorSubject, Observable, Subscription, withLatestFrom } from 'rxjs'
import { applyOperation, getValueByPointer } from './json-patch-lib'

export class PatchDB<T extends { [key: string]: any }> {
  private sub: Subscription | null = null
  private watchedNodes: { [path: string]: BehaviorSubject<any> } = {}

  readonly cache$ = new BehaviorSubject({ sequence: 0, data: {} as T })

  constructor(private readonly source$: Observable<Update<T>[]>) {}

  async start(bootstrapper: Bootstrapper<T>) {
    if (this.sub) return

    const initialCache = await bootstrapper.init()
    this.cache$.next(initialCache)

    this.sub = this.source$
      .pipe(withLatestFrom(this.cache$))
      .subscribe(([updates, cache]) => {
        this.proccessUpdates(updates, cache)
        bootstrapper.update(cache)
      })
  }

  stop() {
    if (!this.sub) return

    Object.values(this.watchedNodes).forEach(node => node.complete())
    this.watchedNodes = {}
    this.sub.unsubscribe()
    this.sub = null
    this.cache$.next({ sequence: 0, data: {} as T })
  }

  watch$(): Observable<T>
  watch$<P1 extends keyof T>(p1: P1): Observable<NonNullable<T[P1]>>
  watch$<P1 extends keyof T, P2 extends keyof NonNullable<T[P1]>>(
    p1: P1,
    p2: P2,
  ): Observable<NonNullable<NonNullable<T[P1]>[P2]>>
  watch$<
    P1 extends keyof T,
    P2 extends keyof NonNullable<T[P1]>,
    P3 extends keyof NonNullable<NonNullable<T[P1]>[P2]>,
  >(
    p1: P1,
    p2: P2,
    p3: P3,
  ): Observable<NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>>
  watch$<
    P1 extends keyof T,
    P2 extends keyof NonNullable<T[P1]>,
    P3 extends keyof NonNullable<NonNullable<T[P1]>[P2]>,
    P4 extends keyof NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>,
  >(
    p1: P1,
    p2: P2,
    p3: P3,
    p4: P4,
  ): Observable<
    NonNullable<NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]>
  >
  watch$<
    P1 extends keyof T,
    P2 extends keyof NonNullable<T[P1]>,
    P3 extends keyof NonNullable<NonNullable<T[P1]>[P2]>,
    P4 extends keyof NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>,
    P5 extends keyof NonNullable<
      NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]
    >,
  >(
    p1: P1,
    p2: P2,
    p3: P3,
    p4: P4,
    p5: P5,
  ): Observable<
    NonNullable<
      NonNullable<NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]>[P5]
    >
  >
  watch$<
    P1 extends keyof T,
    P2 extends keyof NonNullable<T[P1]>,
    P3 extends keyof NonNullable<NonNullable<T[P1]>[P2]>,
    P4 extends keyof NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>,
    P5 extends keyof NonNullable<
      NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]
    >,
    P6 extends keyof NonNullable<
      NonNullable<NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]>[P5]
    >,
  >(
    p1: P1,
    p2: P2,
    p3: P3,
    p4: P4,
    p5: P5,
    p6: P6,
  ): Observable<
    NonNullable<
      NonNullable<
        NonNullable<
          NonNullable<NonNullable<NonNullable<T[P1]>[P2]>[P3]>[P4]
        >[P5]
      >[P6]
    >
  >
  watch$(...args: (string | number)[]): Observable<any> {
    const path = `/${args.join('/')}`

    if (!this.watchedNodes[path]) {
      const data = this.cache$.value.data
      const value = getValueByPointer(data, path)
      this.watchedNodes[path] = new BehaviorSubject(value)
    }

    return this.watchedNodes[path]
  }

  proccessUpdates(updates: Update<T>[], cache: DBCache<T>) {
    updates.forEach(update => {
      if (this.isRevision(update)) {
        const expected = cache.sequence + 1
        if (update.id < expected) return
        if (update.id > expected) {
          return console.error(
            `Received futuristic revision. Expected ${expected}, got ${update.id}`,
          )
        }
        this.handleRevision(update, cache)
      } else {
        this.handleDump(update, cache)
      }
      cache.sequence = update.id
    })
    this.cache$.next(cache)
  }

  private handleRevision(revision: Revision, cache: DBCache<T>): void {
    // apply opperations
    revision.patch.forEach(op => {
      applyOperation(cache, op)
    })
    // update watched nodes
    revision.patch.forEach(op => {
      Object.keys(this.watchedNodes).forEach(watchedPath => {
        const revisionPath = op.path
        if (revisionPath.includes(watchedPath)) {
          this.updateWatchedNode(watchedPath, cache.data)
        }
      })
    })
  }

  private handleDump(dump: Dump<T>, cache: DBCache<T>): void {
    cache.data = { ...dump.value }
    Object.keys(this.watchedNodes).forEach(path => {
      this.updateWatchedNode(path, cache.data)
    })
  }

  private updateWatchedNode(path: string, data: T): void {
    const value = getValueByPointer(data, path)
    this.watchedNodes[path].next(value)
  }

  private isRevision(update: Update<T>): update is Revision {
    return 'patch' in update
  }
}
