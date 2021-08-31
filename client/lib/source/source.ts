import { Observable } from 'rxjs'
import { Store } from '../store'
import { Update } from '../types'

export interface Source<T> {
  watch$ (store?: Store<T>): Observable<Update<T>>
  connectionMade$: Observable<void>
}
