// The reader's Sends in a run, oldest first, in a fixed ring of typed arrays:
// the node each went to, the bucket it was decided in, and the requests it
// admitted and rejected. The requests chart draws one bar per Send from it, and
// the rejection sentence reads it to name the nodes whose hits fill the window.
// Capacity is a handful of windows of Sends; the oldest entry makes room.

const CAPACITY = 256;

export class RequestLog {
  #len = 0;
  #next = 0;
  readonly #node = new Int32Array(CAPACITY);
  readonly #epoch = new Float64Array(CAPACITY);
  readonly #admitted = new Float64Array(CAPACITY);
  readonly #rejected = new Float64Array(CAPACITY);

  /** The Sends kept. */
  get length(): number {
    return this.#len;
  }

  /** Record a Send at `node`, decided in bucket `epoch`. */
  push(node: number, epoch: number, admitted: number, rejected: number): void {
    this.#node[this.#next] = node;
    this.#epoch[this.#next] = epoch;
    this.#admitted[this.#next] = admitted;
    this.#rejected[this.#next] = rejected;
    this.#next = (this.#next + 1) % CAPACITY;
    this.#len = Math.min(this.#len + 1, CAPACITY);
  }

  /** The ring slot of the `i`-th Send kept, oldest first. */
  #at(i: number): number {
    return (this.#next - this.#len + i + CAPACITY) % CAPACITY;
  }

  node(i: number): number {
    return this.#node[this.#at(i)];
  }

  epoch(i: number): number {
    return this.#epoch[this.#at(i)];
  }

  admitted(i: number): number {
    return this.#admitted[this.#at(i)];
  }

  rejected(i: number): number {
    return this.#rejected[this.#at(i)];
  }

  /** The nodes, ascending, that admitted requests in the window's live
   *  buckets: those after `currentEpoch − liveBuckets`. Called only when a
   *  rejection's sentence is built. */
  nodesInWindow(currentEpoch: number, liveBuckets: number): number[] {
    const nodes = new Set<number>();
    for (let i = 0; i < this.#len; i++) {
      if (this.#admitted[i] > 0 && this.#epoch[i] > currentEpoch - liveBuckets) nodes.add(this.#node[i]);
    }
    return [...nodes].sort((a, b) => a - b);
  }
}
