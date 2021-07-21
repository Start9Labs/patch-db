import { Observable } from 'rxjs'
import { Store } from '../store'
import { HashMap, Update } from '../types'

export interface Source<T extends HashMap> {
  watch$ (store?: Store<T>): Observable<Update<T>>
}
