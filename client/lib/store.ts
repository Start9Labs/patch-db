import { DBCache, Dump, Http, Revision, Update } from './types'
import { BehaviorSubject, Observable } from 'rxjs'
import { applyOperation, getValueByPointer, Operation } from './json-patch-lib'
import BTree from 'sorted-btree'

export interface StashEntry {
  revision: Revision
  undo: Operation[]
}

export class Store<T extends { [key: string]: any }> {
  cache: DBCache<T>
  sequence$: BehaviorSubject<number>
  private watchedNodes: { [path: string]: BehaviorSubject<any> } = { }
  private stash = new BTree<number, StashEntry>()

  constructor (
    private readonly http: Http<T>,
    private readonly initialCache: DBCache<T>,
  ) {
    this.cache = this.initialCache
    this.sequence$ = new BehaviorSubject(initialCache.sequence)
  }

  watch$ (): Observable<T>
  watch$<P1 extends keyof T> (p1: P1): Observable<T[P1]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1]> (p1: P1, p2: P2): Observable<T[P1][P2]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2]> (p1: P1, p2: P2, p3: P3): Observable<T[P1][P2][P3]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3]> (p1: P1, p2: P2, p3: P3, p4: P4): Observable<T[P1][P2][P3][P4]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5): Observable<T[P1][P2][P3][P4][P5]>
  watch$<P1 extends keyof T, P2 extends keyof T[P1], P3 extends keyof T[P1][P2], P4 extends keyof T[P1][P2][P3], P5 extends keyof T[P1][P2][P3][P4], P6 extends keyof T[P1][P2][P3][P4][P5]> (p1: P1, p2: P2, p3: P3, p4: P4, p5: P5, p6: P6): Observable<T[P1][P2][P3][P4][P5][P6]>
  watch$ (...args: (string | number)[]): Observable<any> {
    const path = `/${args.join('/')}`
    if (!this.watchedNodes[path]) {
      this.watchedNodes[path] = new BehaviorSubject(getValueByPointer(this.cache.data, path))
    }
    return this.watchedNodes[path].asObservable()
  }

  update (update: Update<T>): void {
    // if old or known, return
    if (update.id <= this.cache.sequence || this.stash.get(update.id)) return

    if (this.isRevision(update)) {
      this.handleRevision(update)
    } else {
      this.handleDump(update)
    }
  }

  reset (): void {
    Object.values(this.watchedNodes).forEach(node => node.complete())
    this.watchedNodes = { }
    this.stash.clear()
  }

  private handleRevision (revision: Revision): void {
    // stash the revision
    this.stash.set(revision.id, { revision, undo: [] })

    // if revision is futuristic, fetch missing revisions
    if (revision.id > this.cache.sequence + 1) {
      this.http.getRevisions(this.cache.sequence)
    }

    this.processStashed(revision.id)
  }

  private handleDump (dump: Dump<T>): void {
    Object.keys(this.cache.data).forEach(key => {
      if (dump.value[key] === undefined) {
        delete this.cache.data[key]
      }
    })

    Object.entries(dump.value).forEach(([key, val]) => {
      (this.cache.data as any)[key] = val
    })

    this.stash.deleteRange(this.cache.sequence, dump.id, false)
    this.updateWatchedNodes('')
    this.updateSequence(dump.id)
    this.processStashed(dump.id + 1)
  }

  private processStashed (id: number): void {
    this.undoRevisions(id)
    this.applyRevisions(id)
  }

  private undoRevisions (id: number): void {
    let stashEntry = this.stash.get(this.stash.maxKey() as number)

    while (stashEntry && stashEntry.revision.id > id) {
      stashEntry.undo.forEach(u => {
        applyOperation(document, u)
      })
      stashEntry = this.stash.nextLowerPair(stashEntry.revision.id)?.[1]
    }
  }

  private applyRevisions (id: number): void {
    let revision = this.stash.get(id)?.revision

    while (revision) {
      let undo: Operation[] = []

      let success = false
      try {
        revision.patch.forEach(op => {
          const u = applyOperation(this.cache.data, op)
          if (u) undo.push(u)
        })
        success = true
      } catch (e) {
        undo.forEach(u => {
          applyOperation(document, u)
        })
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

  private updateWatchedNodes (revisionPath: string) {
    const kill = (path: string) => {
      this.watchedNodes[path].complete()
      delete this.watchedNodes[path]
    }

    Object.keys(this.watchedNodes).forEach(path => {
      if (this.watchedNodes[path].observers.length === 0) return kill(path)

      if (path.includes(revisionPath) || revisionPath.includes(path)) {
        const val = getValueByPointer(this.cache.data, path)
        if (val === undefined) return kill(path)

        this.watchedNodes[path].next(val)
      }
    })
  }

  private updateSequence (sequence: number): void {
    this.cache.sequence = sequence
    this.sequence$.next(sequence)
  }

  private isRevision (update: Update<T>): update is Revision {
    return !!(update as Revision).patch
  }
}