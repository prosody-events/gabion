<script lang="ts">
  // "How to read this": what each word and mark on the page means, in two
  // groups, then how the controls work. From 640 px it opens in the detail
  // pane, so the stage never moves; below, in the page's flow.
  import RoleGlyph from './RoleGlyph.svelte';
</script>

<section class="glossary-pane" id="glossary" aria-labelledby="glossary-title">
  <h2 id="glossary-title">How to read this</h2>
  <h3>Reading the page</h3>
  <dl class="glossary serif">
    <dt>node</dt>
    <dd>
      One gabion server. Its vessel holds the hits it counts in its window, the count in its middle, and its name, "node 4",
      beside it.
    </dd>
    <dt>counting</dt>
    <dd>
      Each node counts its own clients' hits; gossip spreads every node's count to the others, and the cluster's count is the
      sum.
    </dd>
    <dt>hit</dt>
    <dd>One unit of a request's cost; here every request is one hit.</dd>
    <dt>request</dt>
    <dd>One call a client makes to a node. A Send is a group of them, as many as Requests per Send says.</dd>
    <dt>admitted</dt>
    <dd>A request the node let through: its hit is counted in the node's window.</dd>
    <dt>rejected</dt>
    <dd>A request the node turned away, whole: nothing is counted or sent.</dd>
    <dt>hits counted</dt>
    <dd>Every hit admitted anywhere in the cluster that is still in the window: what every node is trying to hear of.</dd>
    <dt>sees</dt>
    <dd>The hits a node has heard of, its own and the others', as in "sees 75 of the 100 limit".</dd>
    <dt>behind</dt>
    <dd>How many counted hits a node has not heard of yet. A node that has heard them all is caught up, and its ring closes.</dd>
    <dt>window</dt>
    <dd>Hits count for the window's length, then every node stops counting them at the same instant.</dd>
    <dt>limit</dt>
    <dd>
      Each node checks every request against the hits it has heard of; a request that would take it over the limit is
      rejected, whole.
    </dd>
    <dt>error budget</dt>
    <dd>
      About one percent of the limit, shared among the other nodes. A node holding more unsent hits than this sends them at
      once, otherwise at its next heartbeat.
    </dd>
    <dt>heartbeat</dt>
    <dd>Each node's regular beat: it sends what it holds to its peers and waits one beat for their acks.</dd>
    <dt>hop</dt>
    <dd>One delivered wave of packets.</dd>
    <dt>Step</dt>
    <dd>Runs to the next event, a wave of packets or the instant hits leave the window, and stops.</dd>
    <dt>selected node</dt>
    <dd>
      <RoleGlyph kind="selected" /> The node Send goes to. Each node picks its own gateway into each other zone, so the ports
      change when you select another node.
    </dd>
    <dt>zone</dt>
    <dd>An availability zone. A node's zone is whatever the node says it is.</dd>
    <dt>inside a zone</dt>
    <dd>Delivery between nodes of one zone: a thin arrow that stays inside its ring.</dd>
    <dt>across zones</dt>
    <dd>Delivery between zones: a thick arrow that crosses the empty centre.</dd>
    <dt>packet</dt>
    <dd>
      A line from the sending node to the receiving one, drawn the instant it is sent and fading over one simulated second:
      Pause holds it, Speed stretches it. Data ends in an arrow; an answer in a dot, a message that asks for one in a ring,
      and a gap report in a square.
    </dd>
    <dt>message</dt>
    <dd>One packet one node sent another. Messages, beside Details at the top of the pane, lists each one.</dd>
    <dt>own data</dt>
    <dd>A node's own changes to its count.</dd>
    <dt>relay data</dt>
    <dd>Other nodes' changes a gateway passes on into its zone.</dd>
    <dt>skip</dt>
    <dd>Changes that expired before they were sent, so the peer can count them as received.</dd>
    <dt>gateway</dt>
    <dd>
      <RoleGlyph kind="port" /> The member of a remote zone that the selected node sends its hits to. It relays them into its
      zone, from the port on its rim. Gateways are each node's own choice, not fixed roles.
    </dd>
    <dt>relay</dt>
    <dd>A gateway forwarding hits it received to the rest of its zone.</dd>
    <dt>ack</dt>
    <dd>"I have your changes through position N." One number acknowledges everything below it.</dd>
    <dt>repeat</dt>
    <dd>Data sent again, after a gap report or after waiting one heartbeat for an ack.</dd>
    <dt>probe</dt>
    <dd>"Answer me", which is also how a node greets a peer it has not heard.</dd>
    <dt>probe answer</dt>
    <dd>The answer to a probe, carrying the answering node's acks.</dd>
    <dt>position</dt>
    <dd>A node's changes are numbered in order; one change can carry many hits.</dd>
    <dt>start</dt>
    <dd>Which start of a node sent a message: a node that restarts counts again from a new start.</dd>
    <dt>in flight</dt>
    <dd>
      In flight is the simulation's own state, while Step has stopped before a wave; a picture of a packet can still be
      moving after it has arrived.
    </dd>
    <dt>ladder</dt>
    <dd>Two nodes' conversation; time runs down, and a slanted rung shows order, not delay.</dd>
    <dt>the frame</dt>
    <dd>A message's bytes as gabion encodes them: a fixed header, then, with data, the cells.</dd>
  </dl>
  <h3>When something goes wrong</h3>
  <dl class="glossary serif">
    <dt>cut off</dt>
    <dd>A node or a zone the network no longer lets a packet reach. Each cut is drawn on the stage and listed, and can be undone.</dd>
    <dt>suspect</dt>
    <dd>
      <RoleGlyph kind="suspect" /> A peer that left something unanswered for the suspect timeout, or a remote peer that said
      it cannot relay. A suspect is not used as a gateway.
    </dd>
    <dt>gap report</dt>
    <dd>Gabion's NAK: "I am missing your changes from position N on; repeat them".</dd>
    <dt>cannot relay</dt>
    <dd>A flag a node sets on every message while it cannot pass hits on into its zone; only a node in another zone acts on it.</dd>
    <dt>backstop</dt>
    <dd>
      A gateway that hears the node sending to it but cannot reach its own zone says it cannot relay, and that node moves to
      another gateway.
    </dd>
    <dt>lost</dt>
    <dd>A message the network dropped: on a cut link, on a link that drops packets, or at a full queue.</dd>
    <dt>join sweep</dt>
    <dd>
      A one-off copy of a node's whole live store, sent to a zone member that just joined: a sweep request asks for it, and
      sweep pages carry it.
    </dd>
  </dl>
  <h3>How the controls work</h3>
  <dl class="glossary serif">
    <dt>the next action</dt>
    <dd>
      The one dark button is what to press next, and the line under the goal names it; when nothing is dark, watch the meter.
    </dd>
    <dt>time</dt>
    <dd>
      A Send pressed while paused plays until every node has the hits, then pauses again; while playing, it keeps playing.
      The node admits the requests in order while its window has room and rejects the rest.
    </dd>
    <dt>the network</dt>
    <dd>Cuts show on the picture, and each one can be undone.</dd>
    <dt>settings</dt>
    <dd>Restart keeps your settings, a scenario button takes its own, and Undo brings back the run you left.</dd>
    <dt>keys</dt>
    <dd>
      Space plays or pauses · → steps to the next event · Esc returns to the scenario's node. In Messages: ↑ ↓ choose a
      message · Enter reads it · Esc goes back. Pause to point at a packet on the stage.
    </dd>
  </dl>
</section>

<style>
  .glossary-pane {
    display: flex;
    flex-direction: column;
    gap: var(--gap-sm);
  }

  h2 {
    margin: 0;
    font-size: var(--font-size-body);
    font-weight: 600;
  }

  h3 {
    margin: var(--gap-sm) 0 0;
    font-size: var(--font-size-caption);
    font-weight: 600;
    font-variant-caps: all-small-caps;
    letter-spacing: 0.06em;
    color: var(--color-fg-muted);
  }

  .glossary {
    display: grid;
    grid-template-columns: max-content minmax(0, var(--measure));
    gap: var(--gap-xs) var(--gap-md);
    margin: 0;
    font-size: var(--font-size-caption);
  }

  .glossary dt {
    font-weight: 600;
  }

  .glossary dd {
    margin: 0;
  }

  /* A narrow pane or the phone: each term over its definition. */
  @container (width < 480px) {
    .glossary {
      grid-template-columns: minmax(0, 1fr);
      gap: 0;
    }

    .glossary dd + dt {
      margin-top: var(--gap-sm);
    }
  }

  @media (width < 640px) {
    .glossary {
      grid-template-columns: minmax(0, 1fr);
      gap: 0;
    }

    .glossary dd + dt {
      margin-top: var(--gap-sm);
    }
  }
</style>
