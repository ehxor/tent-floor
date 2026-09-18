// scanner-feed/src/tier.js
//
// Who may see what.
//
// Every envelope carries a tier. `public` events — CAD, wildfire, tone pages —
// are restructured from APIs the agencies already publish, so republishing them
// is not a new disclosure. `sensitive` events are transcripts: radio is
// broadcast and gone until we write it down, and an indexed archive of it is a
// different object from a live scanner in a kitchen.
//
// So the split is by depth rather than by access. Anyone may tail the feed live
// and look back an hour. Reading further back into transcripts needs a token.
//
// This lives on its own because both the edge (replay) and the Durable Object
// (live tail) have to apply it. An earlier draft filtered only on the replay
// path, which would have streamed every transcript to anyone holding the
// stream open — the exact thing the tiers exist to prevent.
//
// See docs/architecture.md.

export const LIVE_WINDOW_MS = 60 * 60 * 1000;
export const SENSITIVE = "sensitive";
export const PUBLIC = "public";
export const KNOWN_TIERS = new Set([PUBLIC, SENSITIVE]);

/** Whether `tier` names a tier this Worker knows to be freely readable.
 *
 *  Anything else — a miscased "Sensitive", a typo, a tier added to the producer
 *  before the edge learned about it — is treated as sensitive. Failing open
 *  here would publish every transcript at any depth with no error raised in
 *  either direction, which is precisely what the tiers exist to prevent. A
 *  tier we do not recognise is one we cannot vouch for. */
export function isPublicTier(tier) {
  return tier === PUBLIC;
}

/** What `event` looks like to this reader, or null if they may not see it. */
export function forReader(event, { authorised, now = Date.now() }) {
  if (authorised) return event;

  if (!isPublicTier(event.tier)) {
    const age = now - Date.parse(event.ts);
    // NaN (an unparseable ts) fails this comparison, which withholds the event
    // — the safe direction for anything we cannot date.
    if (!(age <= LIVE_WINDOW_MS)) return null;
  }

  const audio = event.data && event.data.audio;
  if (audio && audio.url) {
    // The clip's existence, length and id are fine to advertise; fetching it is
    // what needs a token. Stripping the whole block would leave an
    // unauthenticated consumer unable to tell a transcript with audio from one
    // without.
    return { ...event, data: { ...event.data, audio: { ...audio, url: null } } };
  }
  return event;
}
