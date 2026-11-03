/// <reference types="svelte" />
/// <reference types="vite/client" />

interface Window {
  /** The live simulation, set after every build: the design audit's and the
   *  console's handle on the engine (`sim.snapshot(id)`). */
  sim?: import('./lib/sim/sim').Sim;
}
