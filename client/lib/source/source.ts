import { Observable } from 'rxjs'
import { Store } from '../store'
import { Update } from '../types'
import { RPCResponse } from './ws-source'

export interface Source<T> {
  watch$(store?: Store<T>): Observable<RPCResponse<Update<T>>>
}
