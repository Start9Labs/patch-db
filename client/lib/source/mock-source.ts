import { Observable } from 'rxjs'
import { map } from 'rxjs/operators'
import { Update } from '../types'
import { Source } from './source'
import { RPCResponse } from './ws-source'

export class MockSource<T> implements Source<T> {
  constructor (private readonly seed: Observable<Update<T>>) { }

  watch$ (): Observable<RPCResponse<Update<T>>> {
    return this.seed.pipe(map((result) => ({ result, jsonrpc: '2.0' })))
  }
}
