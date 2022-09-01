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

    return new Observable(subscriber => {
      const data = this.cache$.value.data
      const value = getValueByPointer(data, path)
      const source = this.watchedNodes[path] || new BehaviorSubject(value)
      const subscription = source.subscribe(subscriber)

      this.watchedNodes[path] = source
      this.updateWatchedNode(path, data)

      return () => {
        subscription.unsubscribe()

        if (!source.observed) {
          source.complete()
          delete this.watchedNodes[path]
        }
      }
    })
  }

  proccessUpdates(updates: Update<T>[], cache: DBCache<T>) {
    updates.forEach(update => {
      if (update.id <= cache.sequence) return

      if (this.isRevision(update)) {
        if (update.id > cache.sequence + 1) {
          console.error(
            `Received futuristic revision. Expected ${
              cache.sequence + 1
            }, got ${update.id}`,
          )
          return
        }
        this.handleRevision(update, cache)
      } else {
        this.handleDump(update, cache)
      }
      cache.sequence++
    })
    this.cache$.next(cache)
  }

  private handleRevision(revision: Revision, cache: DBCache<T>): void {
    revision.patch.forEach(op => {
      applyOperation(cache, op)
      this.updateWatchedNodes(op.path, cache.data)
    })
  }

  private handleDump(dump: Dump<T>, cache: DBCache<T>): void {
    cache.data = { ...dump.value }
    this.updateWatchedNodes('', cache.data)
  }

  private updateWatchedNodes(revisionPath: string, data: T) {
    Object.keys(this.watchedNodes).forEach(path => {
      if (path.includes(revisionPath) || revisionPath.includes(path)) {
        this.updateWatchedNode(path, data)
      }
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
