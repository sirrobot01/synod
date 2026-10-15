<script lang="ts" module>
  // One stroke icon set: a 16 by 16 grid, a 1.5 stroke, square caps to match the
  // square geometry of the console, and currentColor. Text next to an icon names
  // the action, so every icon is hidden from assistive technology.
  const drawings = {
    overview: '<path d="M2.5 2.5h4.5v4.5h-4.5zM9 2.5h4.5v4.5h-4.5zM2.5 9h4.5v4.5h-4.5zM9 9h4.5v4.5h-4.5z"/>',
    apps: '<path d="M2.5 3h11v3.5h-11zM2.5 9.5h11V13h-11z"/><path d="M4.5 4.75h1M4.5 11.25h1"/>',
    device: '<path d="M2 3h12v8H2zM5.5 14h5M8 11v3"/>',
    storage: '<ellipse cx="8" cy="4" rx="5.5" ry="2"/><path d="M2.5 4v8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2V4M2.5 8c0 1.1 2.5 2 5.5 2s5.5-.9 5.5-2"/>',
    activity: '<path d="M1.5 8h3l2-5 3 10 2-5h3"/>',
    settings: '<circle cx="8" cy="8" r="2.25"/><path d="M8 1.5v2M8 12.5v2M1.5 8h2M12.5 8h2M3.4 3.4l1.4 1.4M11.2 11.2l1.4 1.4M3.4 12.6l1.4-1.4M11.2 4.8l1.4-1.4"/>',
    close: '<path d="M4 4l8 8M12 4l-8 8"/>',
    plus: '<path d="M8 3v10M3 8h10"/>',
    sun: '<circle cx="8" cy="8" r="2.75"/><path d="M8 1v1.75M8 13.25V15M1 8h1.75M13.25 8H15M3.05 3.05l1.24 1.24M11.71 11.71l1.24 1.24M3.05 12.95l1.24-1.24M11.71 4.29l1.24-1.24"/>',
    moon: '<path d="M13.5 9.75A5.75 5.75 0 016.25 2.5a5.75 5.75 0 107.25 7.25z"/>',
    restore: '<path d="M2.75 8a5.25 5.25 0 105.25-5.25c-1.9 0-3.4 1-4.4 2.4"/><path d="M3.25 2v3.5h3.5"/>',
    backup: '<path d="M8 11V2.5M4.75 5.75L8 2.5l3.25 3.25M2.5 10v3.5h11V10"/>',
    download: '<path d="M8 2.5V11M4.75 7.75L8 11l3.25-3.25M2.5 10v3.5h11V10"/>',
    folder: '<path d="M1.5 3h4.5l1.5 1.75h7v8.75h-13z"/>',
    test: '<path d="M6 1.5h4M6.75 1.5v4.25L2.5 13.5h11L9.25 5.75V1.5M4.25 10.25h7.5"/>',
    trash: '<path d="M2.5 4h11M6 4V2.25h4V4M4 4l.75 10h6.5L12 4M6.75 6.75v4.5M9.25 6.75v4.5"/>',
    copy: '<path d="M5.5 5.5h8v8h-8z"/><path d="M10.5 5.5v-3h-8v8h3"/>',
    external: '<path d="M9 2.5h4.5V7M13.5 2.5L7.5 8.5M11.5 9.5v4h-9v-9h4"/>',
    left: '<path d="M10 3L5 8l5 5"/>',
    right: '<path d="M6 3l5 5-5 5"/>',
    search: '<circle cx="7" cy="7" r="4.25"/><path d="M10.25 10.25l3.5 3.5"/>',
    filter: '<path d="M2 3h12L9.25 8.5v4.25l-2.5 1.25V8.5z"/>',
    edit: '<path d="M10.5 2.5l3 3-8 8H2.5v-3z"/><path d="M8.75 4.25l3 3"/>',
    refresh: '<path d="M13.25 8a5.25 5.25 0 01-9.3 3.35M2.75 8a5.25 5.25 0 019.3-3.35"/><path d="M12.25 1.75v3h-3M3.75 14.25v-3h3"/>',
    signout: '<path d="M6.5 2.5h-4v11h4M10 5l3 3-3 3M13 8H6"/>',
    check: '<path d="M2.75 8.5l3.25 3.25 7.25-7.5"/>',
    clear: '<path d="M2.5 4h11M2.5 8h7M2.5 12h4M10.5 10.5l3 3M13.5 10.5l-3 3"/>',
  } as const;
  export type IconName = keyof typeof drawings;
</script>

<script lang="ts">
  let { name, size = 16 }: { name: IconName; size?: number } = $props();
</script>

<svg
  class="icon"
  aria-hidden="true"
  focusable="false"
  width={size}
  height={size}
  viewBox="0 0 16 16"
  fill="none"
  stroke="currentColor"
  stroke-width="1.5"
  stroke-linecap="square"
  stroke-linejoin="miter">{@html drawings[name]}</svg>

<style>
  .icon {
    flex-shrink: 0;
    display: block;
  }
</style>
