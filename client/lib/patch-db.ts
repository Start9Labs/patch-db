import { Dump, Revision, Update } from './types'
import {
  BehaviorSubject,
  filter,
  Observable,
  Subscription,
  switchMap,
  take,
  withLatestFrom,
} from 'rxjs'
import {
  applyOperation,
  arrayFromPath,
  getValueByPointer,
  pathFromArray,
} from './json-patch-lib'

export class PatchDB<T extends { [key: string]: any }> {
  private sub: Subscription | null = null
  private watchedNodes: {
    [path: string]: {
      subject: BehaviorSubject<any>
      pathArr: string[]
    }
  } = {}

  readonly cache$ = new BehaviorSubject<Dump<T>>({
    id: 0,
    value: {} as T,
  })

  constructor(private readonly source$: Observable<Update<T>[]>) {}

  start() {
    if (this.sub) return

    this.sub = this.source$
      .pipe(withLatestFrom(this.cache$))
      .subscribe(([updates, cache]) => {
        this.proccessUpdates(updates, cache)
      })
  }

  stop() {
    if (!this.sub) return

    Object.values(this.watchedNodes).forEach(node => node.subject.complete())
    this.watchedNodes = {}
    this.sub.unsubscribe()
    this.sub = null
    this.cache$.next({ id: 0, value: {} as T })
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
    return this.cache$.pipe(
      filter(({ id }) => !!id),
      take(1),
      switchMap(({ value }) => {
        const path = pathFromArray(args)
        if (!this.watchedNodes[path]) {
          this.watchedNodes[path] = {
            subject: new BehaviorSubject(getValueByPointer(value, path)),
            pathArr: arrayFromPath(path),
          }
        }
        return this.watchedNodes[path].subject
      }),
    )
  }

  proccessUpdates(updates: Update<T>[], cache: Dump<T>) {
    updates.forEach(update => {
      if (this.isRevision(update)) {
        const expected = cache.id + 1
        if (update.id < expected) return
        this.handleRevision(update, cache)
      } else {
        this.handleDump(update, cache)
      }
      cache.id = update.id
    })
    this.cache$.next(cache)
  }

  private handleRevision(revision: Revision, cache: Dump<T>): void {
    // apply opperations
    revision.patch.forEach(op => {
      applyOperation(cache, op)
    })
    // update watched nodes
    Object.entries(this.watchedNodes).forEach(([watchedPath, { pathArr }]) => {
      const match = revision.patch.find(({ path }) => {
        const arr = arrayFromPath(path)
        return startsWith(pathArr, arr) || startsWith(arr, pathArr)
      })
      if (match) this.updateWatchedNode(watchedPath, cache.value)
    })
  }

  private handleDump(dump: Dump<T>, cache: Dump<T>): void {
    cache.value = { ...dump.value }
    Object.keys(this.watchedNodes).forEach(watchedPath => {
      this.updateWatchedNode(watchedPath, cache.value)
    })
  }

  private updateWatchedNode(path: string, data: T): void {
    const value = getValueByPointer(data, path)
    this.watchedNodes[path].subject.next(value)
  }

  private isRevision(update: Update<T>): update is Revision {
    return 'patch' in update
  }
}

function startsWith(a: string[], b: string[]) {
  for (let i = 0; i < b.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
