// Temporary R2 probe: drive the real wasm artifact under node to see whether
// the `spawn_local` + `tokio::time` engine composition runs or panics.
const wasm = require("./pkg/gabion_wasm.js");

async function main() {
  const config = { nodes: 3, tick_interval_ms: 100 };
  console.log("constructing Sim...");
  const sim = new wasm.Sim(config);
  console.log("Sim constructed");

  console.log("submit_request(0, 1, 5)...");
  const batch = await sim.submit_request(0, 1n, 5n);
  console.log("submit batch:", JSON.stringify(batch));

  console.log("step(500)...");
  const stepBatch = await sim.step(500n, "Time");
  console.log(
    "step -> events:", stepBatch.events.length,
    "virtual_ms:", stepBatch.virtual_ms,
  );

  console.log("snapshot(null, 1, 1)...");
  const snap = await sim.snapshot(null, 1n, 1n);
  console.log(
    "snapshot -> nodes:", snap.nodes.length,
    "oracle_total:", snap.oracle_total,
    "windows:", snap.nodes.map((n) => n.window_total),
  );

  await sim.shutdown();
  console.log("DONE OK");
}

main().catch((e) => {
  console.error("SMOKE FAILED:", e);
  process.exit(1);
});
