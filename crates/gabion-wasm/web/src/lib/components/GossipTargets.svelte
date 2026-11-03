<script lang="ts">
  import { fmtCount, fmtDuration, noun, zoneLabel } from '../format';
  import { ANSWER, type NodeState, type PeerView, type ZoneMode, type ZoneView } from '../sim/types';
  import InfoTip from './InfoTip.svelte';

  // Who the selected node sends its changes to, and how that is going: its own
  // zone, the peers in it, one row per remote zone naming the gateway the node's
  // runtime chose, the counters of its failovers and resends, and the error
  // budget. Every figure is the runtime's own (the admin snapshot the engine
  // reads); the page chooses nothing.
  let {
    node,
    peers,
    zones,
    zoneMode,
  }: {
    node: NodeState;
    peers: PeerView[];
    zones: ZoneView[];
    zoneMode: ZoneMode;
  } = $props();

  const own = $derived(zones[node.zone]);
  // A peer whose zone is still unknown counts among the zone's: it is sent
  // directly until it answers.
  const zonePeers = $derived(peers.filter((p) => p.role === 'zone' || p.role === 'unheard').length);
  const unheard = $derived(peers.filter((p) => p.role === 'unheard').length);
  // One row per remote zone that has members, in zone order.
  const remote = $derived(
    zones.flatMap((zone, z) =>
      z !== node.zone && zone.live > 0
        ? [{ z, name: zoneLabel(zone.name), gateway: peers.find((p) => p.role === 'gateway' && p.zone === z) }]
        : [],
    ),
  );
</script>

<table class="targets">
  <tbody>
    <tr class="own-zone">
      <th scope="row">Own zone</th>
      <td>
        {#if zoneMode === 'Unset'}
          no zone set: this node sends every change to every peer directly
        {:else}
          {zoneLabel(own.name)} · <span data-field="members">{fmtCount(own.live)}</span>
          {noun(own.live, 'node')}
        {/if}
      </td>
    </tr>
    <tr class="zone-peers">
      <th scope="row">Zone peers</th>
      <td>
        <span data-field="count">{fmtCount(zonePeers)}</span>
        {#if unheard > 0}
          · <span data-field="unheard">{fmtCount(unheard)}</span> zone not heard yet (sent directly)
        {/if}
      </td>
    </tr>
    {#if zoneMode === 'Configured'}
      {#each remote as { z, name, gateway } (z)}
        <tr class="remote" data-zone={z}>
          <th scope="row">{name}</th>
          {#if gateway === undefined}
            <td>no gateway yet</td>
          {:else}
            <td>
              <span aria-hidden="true">{ANSWER[gateway.answer].suspect ? '◇' : '◆'}</span>
              gateway node <span data-field="gateway">{gateway.id ?? '—'}</span> ·
              <span data-field="unacked">{fmtCount(gateway.own_unacked)}</span>
              {noun(gateway.own_unacked, 'change')} not yet acknowledged ·
              {#if gateway.unanswered_ms !== undefined}
                waiting <span data-field="waiting">{fmtDuration(gateway.unanswered_ms)}</span>
              {:else}
                answered
              {/if}
              · {ANSWER[gateway.answer].label}
            </td>
          {/if}
        </tr>
      {/each}
    {/if}
    <tr class="counters">
      <th scope="row">Counters</th>
      <td>
        <span data-field="failovers">{fmtCount(node.gateway_failovers)}</span> gateway
        {noun(node.gateway_failovers, 'change')} ·
        <span data-field="gap-reports">{fmtCount(node.gap_reports_sent)}</span> gap
        {noun(node.gap_reports_sent, 'report')} sent ·
        <span data-field="retransmissions">{fmtCount(node.rto_rewinds_own + node.rto_rewinds_relay)}</span>
        {noun(node.rto_rewinds_own + node.rto_rewinds_relay, 'retransmission')} ·
        <span data-field="sweeps">{fmtCount(node.sweeps_served)}</span>
        {noun(node.sweeps_served, 'sweep')} served
      </td>
    </tr>
    <tr class="budget">
      <th scope="row">
        <InfoTip text="How many unsent local hits per rule this node may hold before it sends early, instead of waiting for the next heartbeat.">
          Error budget
        </InfoTip>
      </th>
      <td>
        {#if node.error_budget > 0}
          ε <span data-field="error-budget">{fmtCount(node.error_budget)}</span> hits per rule before an eager flush
        {:else}
          ε set on this node's first request
        {/if}
      </td>
    </tr>
  </tbody>
</table>

<style>
  .targets {
    width: 100%;
    border-collapse: collapse;
    font-size: var(--font-size-caption);
  }

  th,
  td {
    padding: var(--gap-xs) var(--gap-sm) var(--gap-xs) 0;
    border-bottom: 1px solid var(--color-border-line);
    text-align: left;
    vertical-align: baseline;
  }

  th {
    width: 6.5rem;
    font-weight: 600;
    color: var(--color-fg-muted);
  }
</style>
