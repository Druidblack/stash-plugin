(() => {
  "use strict";

  // UI part merged from the former "Open in Jellyfin" plugin.
  // It deliberately uses the existing Jellyfin Sync setting
  // `jellyfinBaseUrl`, so no second base-URL setting is required.

  const HOST_SPAN_ID = "jellyfin_sync__open_host";
  const BTN_ID = "jellyfin_sync__open_btn";
  const COVER_BTN_ID = "jellyfin_sync__cover_btn";
  const BTN_TITLE = "Open in Jellyfin";
  const COVER_BTN_TITLE = "Generate and upload Jellyfin cover";

  const JELLYFIN_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512"
     aria-hidden="true" focusable="false"
     style="width:1em;height:1em;display:block;">
  <defs>
    <linearGradient id="jellyfin-sync-gradient" gradientUnits="userSpaceOnUse"
                    x1="126.15" y1="219.32" x2="457.68" y2="410.73">
      <stop offset="0%" stop-color="#aa5cc3"/>
      <stop offset="100%" stop-color="#00a4dc"/>
    </linearGradient>
  </defs>
  <path fill="url(#jellyfin-sync-gradient)"
        d="M190.56 329.07c8.63 17.3 122.4 17.12 130.93 0 8.52-17.1-47.9-119.78-65.46-119.8-17.57 0-74.1 102.5-65.47 119.8z"/>
  <path fill="url(#jellyfin-sync-gradient)"
        d="M58.75 417.03c25.97 52.15 368.86 51.55 394.55 0S308.93 56.08 256.03 56.08c-52.92 0-223.25 308.8-197.28 360.95zm68.04-45.25c-17.02-34.17 94.6-236.5 129.26-236.5 34.67 0 146.1 202.7 129.26 236.5-16.83 33.8-241.5 34.17-258.52 0z"/>
</svg>`.trim();

  const COVER_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 576 512"
     aria-hidden="true" focusable="false"
     style="width:1em;height:1em;display:block;">
  <path fill="currentColor" d="M480 416H96c-17.7 0-32-14.3-32-32V128c0-17.7 14.3-32 32-32H224l32 48H480c17.7 0 32 14.3 32 32V384c0 17.7-14.3 32-32 32zM160 224c-35.3 0-64 28.7-64 64s28.7 64 64 64c20.4 0 38.6-9.6 50.3-24.5l22.7 27.2c3 3.6 7.4 5.6 12 5.6H416c8.8 0 16-7.2 16-16s-7.2-16-16-16H252.5l-29.9-35.9C223.5 286 224 281.1 224 276c0-28-12.8-53-32.9-69.5c-9.1-7.5-22.5-6.2-30 2.9s-6.2 22.5 2.9 30C170.8 245 176 256.1 176 268.6c0 8.4-2.4 16.2-6.4 22.9C166.7 296.6 163.4 288 160 288c-17.7 0-32-14.3-32-32s14.3-32 32-32c3.4 0 6.7 .6 9.6 1.7c.2 .1 .5 .2 .7 .3c10.9 4 19.2 13.5 22 25.2c2.2 9.1 11.3 14.7 20.4 12.5s14.7-11.3 12.5-20.4c-5.7-23.6-22.3-43-44.3-51.3c-6.6-2.6-13.7-4-20.9-4z"/>
</svg>`.trim();

  let lastLocationKey = null;
  let playbackSyncEnabled = false;
  let operationPluginId = "JellyfinSync";
  const playbackTimers = new Map();
  const lastPlaybackSent = new Map();
  const lastKnownPlayback = new Map();
  const lastSceneActivityAt = new Map();
  const stashResumeFallbackTimers = new Map();
  const attachedMedia = new WeakSet();
  let fetchObserverInstalled = false;

  function log(...args) {
    console.log("[JellyfinSync UI]", ...args);
  }

  function normalizeBaseUrl(value) {
    if (!value) return "";
    return String(value).trim().replace(/\/+$/, "");
  }

  function boolSetting(value) {
    if (typeof value === "boolean") return value;
    return ["1", "true", "yes", "on"].includes(String(value ?? "").trim().toLowerCase());
  }

  function getPluginIdFromScriptUrl() {
    const src = document.currentScript?.src || "";
    const match = src.match(/\/plugin\/([^/]+)\//i);
    return match?.[1] || "";
  }

  function getSceneIdFromLocation(locationObj) {
    const path = locationObj?.pathname || window.location.pathname || "";
    let match = path.match(/\/scenes\/(\d+)/);
    if (match) return parseInt(match[1], 10);

    const hash = window.location.hash || "";
    match = hash.match(/\/scenes\/(\d+)/);
    if (match) return parseInt(match[1], 10);

    return null;
  }

  function getGqlEndpoint() {
    return localStorage.getItem("apiEndpoint") || "/graphql";
  }

  function getApiKey() {
    return localStorage.getItem("apiKey") || null;
  }

  async function gql(query, variables) {
    const headers = {
      "Content-Type": "application/json",
      "Accept": "application/graphql-response+json, application/json",
    };
    const apiKey = getApiKey();
    if (apiKey) headers.Authorization = `Bearer ${apiKey}`;

    const response = await fetch(getGqlEndpoint(), {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ query, variables }),
    });

    if (!response.ok) {
      throw new Error(`GraphQL HTTP ${response.status}`);
    }

    const json = await response.json();
    if (json?.errors?.length) {
      throw new Error(json.errors.map((error) => error.message).join("; "));
    }
    return json.data;
  }

  async function getJellyfinBaseUrl() {
    const data = await gql(`
      query JellyfinSyncConfiguration {
        configuration { plugins }
      }
    `);

    const plugins = data?.configuration?.plugins || {};
    const scriptPluginId = getPluginIdFromScriptUrl();
    const candidates = [scriptPluginId, "JellyfinSync", "jellyfin_sync"].filter(Boolean);

    for (const key of candidates) {
      const config = plugins?.[key];
      const baseUrl = normalizeBaseUrl(config?.jellyfinBaseUrl || config?.baseUrl);
      if (baseUrl) {
        playbackSyncEnabled = boolSetting(config?.syncPlaybackPosition);
        operationPluginId = key;
        return baseUrl;
      }
    }

    // Last-resort compatibility: locate our config by its unique setting name.
    for (const [key, config] of Object.entries(plugins)) {
      const baseUrl = normalizeBaseUrl(config?.jellyfinBaseUrl);
      if (baseUrl) {
        playbackSyncEnabled = boolSetting(config?.syncPlaybackPosition);
        operationPluginId = key;
        return baseUrl;
      }
    }

    playbackSyncEnabled = false;
    return "";
  }

  async function getSceneUrls(sceneId) {
    const data = await gql(`
      query JellyfinSyncSceneUrls($id: ID!) {
        findScene(id: $id) { id urls }
      }
    `, { id: sceneId });
    return data?.findScene?.urls || [];
  }

  async function getSceneResumeTime(sceneId) {
    const data = await gql(`
      query JellyfinSyncSceneResume($id: ID!) {
        findScene(id: $id) { id resume_time }
      }
    `, { id: sceneId });
    const value = Number(data?.findScene?.resume_time ?? 0);
    return Number.isFinite(value) && value > 0 ? value : 0;
  }

  async function runGenerateCover(sceneId) {
    if (!sceneId) return;

    const data = await gql(`
      mutation JellyfinSyncGenerateCover($pluginId: ID!, $args: Map) {
        runPluginOperation(plugin_id: $pluginId, args: $args)
      }
    `, {
      pluginId: operationPluginId || "JellyfinSync",
      args: {
        mode: "generateSceneCover",
        sceneId: String(sceneId),
      },
    });

    return data?.runPluginOperation;
  }

  async function runPlaybackSync(sceneId, resumeTime, source = "player") {
    if (!playbackSyncEnabled || !sceneId) return;

    const data = await gql(`
      mutation JellyfinSyncPlayback($pluginId: ID!, $args: Map) {
        runPluginOperation(plugin_id: $pluginId, args: $args)
      }
    `, {
      pluginId: operationPluginId || "JellyfinSync",
      args: {
        mode: "syncPlaybackPosition",
        sceneId: String(sceneId),
        resumeTime: Number(resumeTime) || 0,
        source: String(source || "player"),
      },
    });

    return data?.runPluginOperation;
  }

  function normalizeResumeTime(value, duration = 0, ended = false) {
    let seconds = Number(value);
    if (!Number.isFinite(seconds) || seconds < 0) seconds = 0;

    const total = Number(duration);
    // Match Stash trackActivity: >=98% completed is stored as resume_time=0.
    if (ended || (Number.isFinite(total) && total > 0 && (seconds / total) * 100 >= 98)) {
      return 0;
    }
    return seconds;
  }

  function queuePlaybackSync(sceneId, resumeTime = null, delayMs = 150, source = "player") {
    if (!playbackSyncEnabled || !sceneId) return;

    if (resumeTime !== null && resumeTime !== undefined) {
      const normalized = Number(resumeTime);
      if (Number.isFinite(normalized) && normalized >= 0) {
        lastKnownPlayback.set(sceneId, normalized);
      }
    }

    const existing = playbackTimers.get(sceneId);
    if (existing) clearTimeout(existing);

    const timer = setTimeout(async () => {
      playbackTimers.delete(sceneId);
      try {
        let seconds = resumeTime;
        if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) {
          seconds = lastKnownPlayback.get(sceneId);
        }
        if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) {
          seconds = await getSceneResumeTime(sceneId);
          source = `${source}:stash-resume`;
        }
        seconds = Math.max(0, Number(seconds) || 0);

        const previous = lastPlaybackSent.get(sceneId);
        if (typeof previous === "number" && Math.abs(previous - seconds) < 1) return;

        const result = await runPlaybackSync(sceneId, seconds, source);
        const resultBody = result?.output ?? result;
        if (resultBody && resultBody.ok === false) {
          throw new Error(resultBody.error || "Jellyfin playback sync failed");
        }
        lastPlaybackSent.set(sceneId, seconds);
        log(`Playback position synced for scene ${sceneId}: ${seconds.toFixed(1)}s (${source})`, result || "");
      } catch (error) {
        log("Unable to sync playback position:", error?.message || error);
      }
    }, delayMs);

    playbackTimers.set(sceneId, timer);
  }

  function currentSceneId() {
    return getSceneIdFromLocation({ pathname: window.location.pathname });
  }

  function mediaCurrentTime(media) {
    if (!media) return null;
    let seconds = Number(media.currentTime);
    if (!Number.isFinite(seconds)) {
      const inner = media.querySelector?.("video.vjs-tech, video");
      seconds = Number(inner?.currentTime);
    }
    return Number.isFinite(seconds) ? seconds : null;
  }

  function mediaDuration(media) {
    if (!media) return 0;
    let duration = Number(media.duration);
    if (!Number.isFinite(duration)) {
      const inner = media.querySelector?.("video.vjs-tech, video");
      duration = Number(inner?.duration);
    }
    return Number.isFinite(duration) ? duration : 0;
  }

  function scheduleStashResumeFallback(sceneId, reason, delayMs = 1200) {
    if (!playbackSyncEnabled || !sceneId) return;

    const existing = stashResumeFallbackTimers.get(sceneId);
    if (existing) clearTimeout(existing);
    const scheduledAt = Date.now();

    const timer = setTimeout(async () => {
      stashResumeFallbackTimers.delete(sceneId);
      try {
        // If Stash itself saved activity after this pause/navigation event, that
        // observer already queued the authoritative resume_time. Do not send a
        // second fallback value.
        const activityAt = lastSceneActivityAt.get(sceneId) || 0;
        if (activityAt >= scheduledAt) return;

        const storedResume = await getSceneResumeTime(sceneId);
        queuePlaybackSync(sceneId, storedResume, 0, `${reason}:stash-resume`);
      } catch (error) {
        log("Unable to read Stash resume position for playback fallback:", error?.message || error);
      }
    }, delayMs);

    stashResumeFallbackTimers.set(sceneId, timer);
  }

  function handleMediaPosition(media, reason, ended = false) {
    if (!playbackSyncEnabled) return;
    const sceneId = currentSceneId();
    if (!sceneId) return;

    // Keep currentTime only as diagnostic/local state. It is deliberately NOT
    // written directly to Jellyfin because Video.js may momentarily expose 0
    // while replacing the media element. The actual sync uses Stash's persisted
    // resume_time from sceneSaveActivity (or the DB fallback below).
    const seconds = mediaCurrentTime(media);
    if (seconds !== null) {
      const normalized = normalizeResumeTime(seconds, mediaDuration(media), ended);
      lastKnownPlayback.set(sceneId, normalized);
    }
    scheduleStashResumeFallback(sceneId, reason, ended ? 300 : 1200);
  }

  function attachPlaybackListeners(root = document) {
    const nodes = root.querySelectorAll?.("video, video-js, .video-js") || [];
    for (const media of nodes) {
      if (attachedMedia.has(media)) continue;
      attachedMedia.add(media);

      media.addEventListener("timeupdate", () => {
        const sceneId = currentSceneId();
        const seconds = mediaCurrentTime(media);
        if (sceneId && seconds !== null) lastKnownPlayback.set(sceneId, seconds);
      }, { passive: true });

      media.addEventListener("pause", () => handleMediaPosition(media, "pause"), { passive: true });
      media.addEventListener("ended", () => handleMediaPosition(media, "ended", true), { passive: true });
    }
  }

  // Capture-phase fallback catches native media events even though pause/ended do not bubble.
  document.addEventListener("pause", (event) => {
    const target = event.target;
    if (target?.tagName === "VIDEO") handleMediaPosition(target, "pause-capture");
  }, true);
  document.addEventListener("ended", (event) => {
    const target = event.target;
    if (target?.tagName === "VIDEO") handleMediaPosition(target, "ended-capture", true);
  }, true);

  const playbackObserver = new MutationObserver(() => attachPlaybackListeners(document));
  playbackObserver.observe(document.documentElement, { childList: true, subtree: true });
  attachPlaybackListeners(document);

  function parseSceneSaveActivity(body) {
    try {
      if (typeof body !== "string") return null;
      const parsed = JSON.parse(body);
      const requests = Array.isArray(parsed) ? parsed : [parsed];
      for (const request of requests) {
        const query = String(request?.query || "");
        if (!query.includes("sceneSaveActivity")) continue;
        const vars = request?.variables || {};
        const sceneId = vars.id ?? vars.scene_id ?? vars.sceneId;
        const resume = vars.resume_time ?? vars.resumeTime;
        if (sceneId != null && resume != null) {
          return { sceneId: Number(sceneId), resumeTime: Number(resume) };
        }
      }
    } catch (_) {
      // Not a JSON GraphQL body; ignore.
    }
    return null;
  }

  function installSceneActivityFetchObserver() {
    if (fetchObserverInstalled || typeof window.fetch !== "function") return;
    fetchObserverInstalled = true;
    const originalFetch = window.fetch.bind(window);

    window.fetch = async function jellyfinSyncObservedFetch(input, init = {}) {
      const activity = parseSceneSaveActivity(init?.body);
      const response = await originalFetch(input, init);

      if (activity && response?.ok && playbackSyncEnabled) {
        const activeSceneId = currentSceneId();
        // Only act on the scene currently being watched. This prevents unrelated
        // GraphQL activity changes from triggering Jellyfin writes.
        if (activeSceneId && Number(activity.sceneId) === Number(activeSceneId)) {
          const media = document.querySelector("video.vjs-tech, video");
          const isPaused = !media || media.paused || media.ended || document.hidden;
          lastKnownPlayback.set(activeSceneId, Math.max(0, activity.resumeTime || 0));
          lastSceneActivityAt.set(activeSceneId, Date.now());
          if (isPaused) {
            queuePlaybackSync(activeSceneId, activity.resumeTime, 0, "sceneSaveActivity");
          }
        }
      }
      return response;
    };
  }

  installSceneActivityFetchObserver();

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden || !playbackSyncEnabled) return;
    const sceneId = currentSceneId();
    if (!sceneId) return;
    const media = document.querySelector("video.vjs-tech, video");
    if (media) handleMediaPosition(media, "visibility-hidden");
  });

  function urlString(value) {
    if (typeof value === "string") return value.trim();
    if (value && typeof value === "object") {
      return String(value.url || value.URL || value.link || value.value || "").trim();
    }
    return "";
  }

  function pickMatchingUrl(urls, baseUrl) {
    const base = normalizeBaseUrl(baseUrl);
    if (!base) return null;

    for (const value of urls || []) {
      const url = urlString(value);
      if (!url) continue;
      if (url === base || url.startsWith(`${base}/`)) return url;
    }
    return null;
  }

  function removeButtonIfExists() {
    document.getElementById(HOST_SPAN_ID)?.remove();
  }

  function createIconNode(svgMarkup = JELLYFIN_SVG) {
    const template = document.createElement("template");
    template.innerHTML = svgMarkup;
    return template.content.firstElementChild;
  }

  function findToolbarGroupWithViews() {
    const groups = Array.from(document.querySelectorAll("span.scene-toolbar-group"));
    for (const group of groups) {
      const eye =
        group.querySelector('div.count-button.increment-only.btn-group svg[data-icon="eye"]') ||
        group.querySelector("div.count-button.increment-only.btn-group .fa-eye") ||
        group.querySelector('div.count-button.increment-only.btn-group button[title*="Views"]') ||
        group.querySelector('div.count-button.increment-only.btn-group button[title*="Счетчик"]');
      if (eye) return group;
    }
    return null;
  }

  function upsertButton(urlToOpen, sceneId) {
    const toolbarGroup = findToolbarGroupWithViews();
    if (!toolbarGroup) return false;

    if (!urlToOpen) {
      removeButtonIfExists();
      return true;
    }

    const eye =
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group svg[data-icon="eye"]') ||
      toolbarGroup.querySelector("div.count-button.increment-only.btn-group .fa-eye") ||
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group button[title*="Views"]') ||
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group button[title*="Счетчик"]');

    const referenceSpan = eye?.closest("span");
    if (!referenceSpan) return false;

    let host = document.getElementById(HOST_SPAN_ID);
    if (!host) {
      host = document.createElement("span");
      host.id = HOST_SPAN_ID;

      const group = document.createElement("div");
      group.setAttribute("role", "group");
      group.className = "btn-group";

      const button = document.createElement("button");
      button.id = BTN_ID;
      button.type = "button";
      button.className = "minimal btn btn-secondary";
      button.title = BTN_TITLE;
      button.setAttribute("aria-label", BTN_TITLE);
      button.appendChild(createIconNode(JELLYFIN_SVG));

      const coverButton = document.createElement("button");
      coverButton.id = COVER_BTN_ID;
      coverButton.type = "button";
      coverButton.className = "minimal btn btn-secondary";
      coverButton.title = COVER_BTN_TITLE;
      coverButton.setAttribute("aria-label", COVER_BTN_TITLE);
      coverButton.appendChild(createIconNode(COVER_SVG));

      group.appendChild(button);
      group.appendChild(coverButton);
      host.appendChild(group);
      toolbarGroup.insertBefore(host, referenceSpan);
    }

    const button = document.getElementById(BTN_ID);
    if (button) {
      button.onclick = (event) => {
        event.preventDefault();
        event.stopPropagation();
        window.open(urlToOpen, "_blank", "noopener,noreferrer");
      };
      button.title = `${BTN_TITLE}
${urlToOpen}`;
    }

    const coverButton = document.getElementById(COVER_BTN_ID);
    if (coverButton) {
      coverButton.disabled = false;
      coverButton.onclick = async (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!sceneId) return;

        const originalTitle = coverButton.title;
        const originalAria = coverButton.getAttribute("aria-label") || COVER_BTN_TITLE;
        const originalHtml = coverButton.innerHTML;
        coverButton.disabled = true;
        coverButton.title = "Generating Jellyfin cover...";
        coverButton.setAttribute("aria-label", "Generating Jellyfin cover...");
        coverButton.textContent = "…";

        try {
          const result = await runGenerateCover(sceneId);
          const resultBody = result?.output ?? result;
          if (resultBody && resultBody.ok === false) {
            throw new Error(resultBody.error || resultBody.message || "Jellyfin cover generation failed");
          }
          log(`Generated Jellyfin cover for scene ${sceneId}`, result || "");
          coverButton.title = "Generated and uploaded Jellyfin cover";
          coverButton.setAttribute("aria-label", "Generated and uploaded Jellyfin cover");
        } catch (error) {
          const message = error?.message || String(error);
          log("Unable to generate Jellyfin cover:", message);
          coverButton.title = `Cover generation failed: ${message}`;
          coverButton.setAttribute("aria-label", `Cover generation failed: ${message}`);
        } finally {
          setTimeout(() => {
            coverButton.disabled = false;
            coverButton.title = COVER_BTN_TITLE;
            coverButton.setAttribute("aria-label", originalAria);
            coverButton.innerHTML = originalHtml;
          }, 1500);
        }
      };
    }

    return true;
  }

  async function renderForScene(sceneId) {
    try {
      const [baseUrl, urls] = await Promise.all([
        getJellyfinBaseUrl(),
        getSceneUrls(sceneId),
      ]);

      if (!baseUrl) {
        removeButtonIfExists();
        return;
      }

      const matchingUrl = pickMatchingUrl(urls, baseUrl);
      if (!matchingUrl) {
        removeButtonIfExists();
        return;
      }

      // React can rebuild the toolbar after navigation. Retry only DOM insertion;
      // GraphQL is not repeated during these retries.
      let tries = 0;
      const maxTries = 40;
      const tick = () => {
        tries += 1;
        if (!upsertButton(matchingUrl, sceneId) && tries < maxTries) {
          setTimeout(tick, 100);
        }
      };
      tick();
    } catch (error) {
      log("Unable to render Open in Jellyfin button:", error?.message || error);
      removeButtonIfExists();
    }
  }

  function handleLocation(locationObj, force = false) {
    const sceneId = getSceneIdFromLocation(locationObj);
    const key = sceneId ? `scene:${sceneId}` : "other";
    if (!force && key === lastLocationKey) return;
    lastLocationKey = key;

    if (!sceneId) {
      removeButtonIfExists();
      return;
    }
    renderForScene(sceneId);
    attachPlaybackListeners(document);
  }

  handleLocation({ pathname: window.location.pathname }, true);

  if (window.PluginApi?.Event?.addEventListener) {
    PluginApi.Event.addEventListener("stash:location", (event) => {
      const locationObj = event?.detail?.data?.location || {
        pathname: window.location.pathname,
      };
      handleLocation(locationObj, true);
    });
  } else {
    // Compatibility fallback without hammering GraphQL every second: only
    // rerender when the route actually changes.
    setInterval(() => handleLocation({ pathname: window.location.pathname }), 1000);
  }
})();
