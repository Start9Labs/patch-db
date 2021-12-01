import { Observable } from 'rxjs'
import { Update } from '../types'
import { Source } from './source'

export class MockSource<T> implements Source<T> {

  constructor (
    private readonly seed: Observable<Update<T>>,
  ) { }

  watch$ (): Observable<Update<T>> {
    return this.seed
  }
}
