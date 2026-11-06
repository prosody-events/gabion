// The requests a run admitted, newest last, in a fixed ring of typed arrays:
// which node admitted in which bucket. The rejection sentence
// reads it to name the nodes whose hits fill the window. Capacity is a
// handful of windows of Sends; the oldest entry makes room.

const CAPACITY = 256;

export class RequestLog {
  #len = 0;
  #next = 0;
  readonly #node = new Int32Array(CAPACITY);
  readonly #epoch = new Float64Array(CAPACITY);

  /** Record a request admitted at `node` in bucket `epoch`. */
  push(node: number, epoch: number): void {
    this.#node[this.#next] = node;
    this.#epoch[this.#next] = epoch;
    this.#next = (this.#next + 1) % CAPACITY;
    this.#len = Math.min(this.#len + 1, CAPACITY);
  }

  /** The nodes, ascending, that admitted a request in the window's live
   *  buckets: those after `currentEpoch − liveBuckets`. Called only when a
   *  rejection's sentence is built. */
  nodesInWindow(currentEpoch: number, liveBuckets: number): number[] {
    const nodes = new Set<number>();
    for (let i = 0; i < this.#len; i++) {
      if (this.#epoch[i] > currentEpoch - liveBuckets) nodes.add(this.#node[i]);
    }
    return [...nodes].sort((a, b) => a - b);
  }
}
