import { DBCache, Dump, Revision, Update } from './types'
import { BehaviorSubject, Observable, ReplaySubject } from 'rxjs'
import { applyOperation, getValueByPointer, Operation } from './json-patch-lib'
import BTree from 'sorted-btree'

export interface StashEntry {
  revision: Revision
  undo: Operation<unknown>[]
}

export class Store<T extends { [key: string]: any }> {
  readonly sequence$ = new BehaviorSubject(this.cache.sequence)
  private watchedNodes: { [path: string]: ReplaySubject<any> } = {}
  private stash = new BTree<number, StashEntry>()

  constructor(public cache: DBCache<T>) {}

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
      this.watchedNodes[path] = new ReplaySubject(1)
      this.updateValue(path)
    }
    return this.watchedNodes[path].asObservable()
  }

  update(update: Update<T>): void {
    if (this.isRevision(update)) {
      // Handle revision if new and not known
      if (update.id > this.cache.sequence && !this.stash.get(update.id)) {
        this.handleRevision(update)
      }
    } else {
      this.handleDump(update)
    }
  }

  reset(): void {
    Object.values(this.watchedNodes).forEach(node => node.complete())
    this.watchedNodes = {}
    this.stash.clear()
    this.sequence$.next(0)
    this.cache = {
      sequence: 0,
      data: {} as any,
    }
  }

  private updateValue(path: string): void {
    const value = getValueByPointer(this.cache.data, path)

    this.watchedNodes[path].next(value)
  }

  private handleRevision(revision: Revision): void {
    this.stash.set(revision.id, { revision, undo: [] })
    this.processStashed(revision.id)
  }

  private handleDump({ value, id }: Dump<T>): void {
    this.cache.data = { ...value }
    this.stash.deleteRange(this.cache.sequence, id, false)
    this.updateWatchedNodes('')
    this.updateSequence(id)
    this.processStashed(id + 1)
  }

  private processStashed(id: number): void {
    this.undoRevisions(id)
    this.applyRevisions(id)
  }

  private undoRevisions(id: number): void {
    let stashEntry = this.stash.get(this.stash.maxKey() as number)

    while (stashEntry && stashEntry.revision.id > id) {
      stashEntry.undo.forEach(u => applyOperation(this.cache, u))
      stashEntry = this.stash.nextLowerPair(stashEntry.revision.id)?.[1]
    }
  }

  private applyRevisions(id: number): void {
    let revision = this.stash.get(id)?.revision
    while (revision) {
      let undo: Operation<unknown>[] = []
      let success = false

      try {
        revision.patch.forEach(op => {
          const u = applyOperation(this.cache, op)
          if (u) undo.push(u)
        })
        success = true
      } catch (e) {
        undo.forEach(u => applyOperation(this.cache, u))
        undo = []
      }

      if (success) {
        revision.patch.forEach(op => {
          this.updateWatchedNodes(op.path)
        })
      }

      if (revision.id === this.cache.sequence + 1) {
        this.updateSequence(revision.id)
      } else {
        this.stash.set(revision.id, { revision, undo })
      }

      // increment revision for next loop
      revision = this.stash.nextHigherPair(revision.id)?.[1].revision
    }

    // delete all old stashed revisions
    this.stash.deleteRange(0, this.cache.sequence, false)
  }

  private updateWatchedNodes(revisionPath: string) {
    const kill = (path: string) => {
      this.watchedNodes[path].complete()
      delete this.watchedNodes[path]
    }

    Object.keys(this.watchedNodes).forEach(path => {
      if (this.watchedNodes[path].observers.length === 0) return kill(path)

      if (path.includes(revisionPath) || revisionPath.includes(path)) {
        this.updateValue(path)
      }
    })
  }

  private updateSequence(sequence: number): void {
    this.cache.sequence = sequence
    this.sequence$.next(sequence)
  }

  private isRevision(update: Update<T>): update is Revision {
    return 'patch' in update
  }
}
