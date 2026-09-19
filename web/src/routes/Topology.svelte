<script lang="ts">
  import { degraded, gaps, quorum, shards, topologyNodes, voters } from "../lib/mock";
  import { follow, path } from "../lib/router.svelte";
  import Mark from "../lib/ui/Mark.svelte";
  import Mocked from "../lib/ui/Mocked.svelte";
  import Panel from "../lib/ui/Panel.svelte";

  let { id, onOpenCluster }: { id: string; onOpenCluster: (id: string) => void } = $props();
</script>

<div class="screen">
  <header>
    <h1>Topology — <span class="mono">{id}</span></h1>
    <p class="note">
      Replication and voting are drawn as two separate planes, because they fail separately.
      <a href={path("cluster", id)} onclick={(event) => follow(event, () => onOpenCluster(id))}>Back to the cluster</a>.
    </p>
  </header>

  <Mocked gap={gaps.topology} />

  <section class="panel plane">
    <div class="plane-head">
      <span class="eyebrow">Replication plane</span>
      <span class="note">streaming · 1 primary, 2 replicas</span>
    </div>

    <div class="members">
      {#each topologyNodes as member (member.name)}
        <article class="member" class:hatched={member.stale} class:strong={member.stale}>
          <div class="member-head">
            <Mark mark={member.mark} text={false} />
            <span class="mono name">{member.name}</span>
          </div>
          <span class="badge mono" style="border-color: var(--{member.roleTone}); color: var(--{member.roleTone})">
            {member.role}
          </span>
          <div class="meta mono">
            {#each member.meta as line (line)}
              <span>{line}</span>
            {/each}
          </div>
          <p class="link mono" style="color: var(--{member.linkTone})">{member.link}</p>
        </article>
      {/each}
    </div>

    <div class="planes-foot">
      <div class="voters">
        <span class="label">Voter plane · etcd</span>
        <div class="voter-chips">
          {#each voters as voter (voter.label)}
            <span class="voter mono" class:hatched={voter.stale} class:dashed={voter.stale}>
              <Mark mark={voter.mark} shapeOnly title={voter.mark.label} />
              {voter.label}
            </span>
          {/each}
        </div>
      </div>
      <div class="quorum">
        <span class="label">Quorum</span>
        <span class="quorum-line mono">
          <Mark mark={quorum.mark} text={false} />
          {quorum.headline}
        </span>
        <p class="note">{quorum.detail}</p>
      </div>
    </div>
  </section>

  <div class="columns">
    <Panel title={`Degraded conditions · ${degraded.length}`}>
      {#each degraded as entry (entry.title)}
        <div class="row top">
          <Mark mark={entry.mark} text={false} />
          <span class="grow">
            <span class="mono title">{entry.title}</span>
            <span class="detail">{entry.detail}</span>
          </span>
        </div>
      {/each}
    </Panel>

    <Panel title="Redis shard plan · redis-cache">
      <div class="grid shards">
        {#each shards as shard (shard.name)}
          <div class="shard" class:hatched={shard.stale}>
            <div class="shard-head">
              <Mark mark={shard.mark} text={false} />
              <span class="mono">{shard.name}</span>
            </div>
            <div class="meta mono">
              {#each shard.meta as line (line)}
                <span>{line}</span>
              {/each}
            </div>
          </div>
        {/each}
      </div>
      {#snippet foot()}
        Slot ranges are shown literally. A resharding operation shows moving slots as an explicit
        in-flight count, never as a percentage.
      {/snippet}
    </Panel>
  </div>
</div>

<style>
  .screen {
    display: flex;
    flex-direction: column;
    gap: 24px;
    max-width: 1240px;
  }
  header {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  header .note {
    font-size: 13px;
  }

  .plane {
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 18px;
  }
  .plane-head {
    display: flex;
    gap: 14px;
    align-items: baseline;
    flex-wrap: wrap;
  }
  .plane-head .eyebrow {
    text-transform: uppercase;
  }

  .members {
    display: flex;
    gap: 22px;
    flex-wrap: wrap;
    align-items: stretch;
  }
  .member {
    flex: 1 1 250px;
    border: 1px solid var(--line);
    background-color: var(--nav);
    padding: 14px;
    display: flex;
    flex-direction: column;
    gap: 9px;
  }
  .member.strong {
    border-color: var(--line-strong);
  }
  .member-head {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .name {
    font-size: 12px;
    font-weight: 600;
  }
  .badge {
    align-self: flex-start;
    font-size: 10px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    border: 1px solid;
    padding: 2px 7px;
  }
  .meta {
    display: flex;
    flex-direction: column;
    gap: 2px;
    font-size: 10px;
    color: var(--ink-muted);
    line-height: 1.6;
  }
  .member .link {
    margin: 0;
    font-size: 11px;
    border-top: 1px solid var(--line);
    padding-top: 9px;
    line-height: 1.5;
  }

  .planes-foot {
    border-top: 1px solid var(--line);
    padding-top: 14px;
    display: flex;
    gap: 20px;
    flex-wrap: wrap;
  }
  .voters,
  .quorum {
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .voter-chips {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
  }
  .voter {
    display: flex;
    gap: 7px;
    align-items: center;
    font-size: 11px;
    border: 1px solid var(--line-strong);
    padding: 5px 9px;
  }
  /* An unknown voter carries a dashed edge as well as a hatch. */
  .voter.dashed {
    border-style: dashed;
    border-color: var(--ink-faint);
  }
  .quorum-line {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--attn);
  }
  .quorum .note {
    max-width: 46ch;
    font-size: 11px;
  }

  .columns {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(330px, 1fr));
    gap: 20px;
    align-items: start;
  }
  .grow {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
  }
  .title {
    font-size: 12px;
    color: var(--ink);
  }
  .detail {
    font-size: 12px;
    color: var(--ink-soft);
    line-height: 1.5;
  }

  .grid.shards {
    grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  }
  .shard {
    background-color: var(--panel);
    padding: 12px 14px;
    display: flex;
    flex-direction: column;
    gap: 7px;
  }
  .shard-head {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 12px;
  }
</style>
