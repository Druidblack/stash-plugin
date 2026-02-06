(() => {
  "use strict";

  const HOST_SPAN_ID = "open_in_jellyfin__host_span";
  const BTN_ID = "open_in_jellyfin__btn";
  const BTN_TITLE = "Open in Jellyfin";

  const ICON_URL = "https://cdn.jsdelivr.net/gh/Druidblack/jellyfin-icon-metadata@main/icons/jellyfin.svg";

  // Fallback-иконка (inline SVG), если внешний не загрузитс¤
  const FALLBACK_SVG = `
<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink"
     viewBox="0 0 512 512" aria-hidden="true" focusable="false"
     style="width:1em;height:1em;display:block;">
  <defs>
    <path d="M190.56 329.07c8.63 17.3 122.4 17.12 130.93 0 8.52-17.1-47.9-119.78-65.46-119.8-17.57 0-74.1 102.5-65.47 119.8z" id="A"/>
    <linearGradient id="B" gradientUnits="userSpaceOnUse" x1="126.15" y1="219.32" x2="457.68" y2="410.73">
      <stop offset="0%" stop-color="#aa5cc3"/>
      <stop offset="100%" stop-color="#00a4dc"/>
    </linearGradient>
    <path d="M58.75 417.03c25.97 52.15 368.86 51.55 394.55 0S308.93 56.08 256.03 56.08c-52.92 0-223.25 308.8-197.28 360.95zm68.04-45.25c-17.02-34.17 94.6-236.5 129.26-236.5 34.67 0 146.1 202.7 129.26 236.5-16.83 33.8-241.5 34.17-258.52 0z" id="C"/>
  </defs>
  <use xlink:href="#A" fill="url(#B)"/>
  <use xlink:href="#C" fill="url(#B)"/>
</svg>`.trim();

  function log(...args) {
    console.log("[open_in_jellyfin]", ...args);
  }

  function getPluginIdFromScriptUrl() {
    const s = document.currentScript?.src || "";
    const m = s.match(/\/plugin\/([^/]+)\//);
    return m?.[1] || "open_in_jellyfin";
  }

  function normalizeBaseUrl(v) {
    if (!v) return "";
    return String(v).trim().replace(/\/+$/, "");
  }

  function getSceneIdFromLocation(loc) {
    const path = loc?.pathname || window.location.pathname || "";
    let m = path.match(/\/scenes\/(\d+)/);
    if (m) return parseInt(m[1], 10);

    const hash = window.location.hash || "";
    m = hash.match(/\/scenes\/(\d+)/);
    if (m) return parseInt(m[1], 10);

    return null;
  }

  // --- GraphQL ---------------------------------------------------------------

  function getGqlEndpoint() {
    return localStorage.getItem("apiEndpoint") || "/graphql";
  }

  function getApiKey() {
    return localStorage.getItem("apiKey") || null;
  }

  async function gql(query, variables) {
    const endpoint = getGqlEndpoint();
    const headers = {
      "Content-Type": "application/json",
      "Accept": "application/graphql-response+json, application/json",
    };
    const apiKey = getApiKey();
    if (apiKey) headers["Authorization"] = `Bearer ${apiKey}`;

    const res = await fetch(endpoint, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ query, variables }),
    });

    const json = await res.json();
    if (json?.errors?.length) {
      throw new Error(json.errors.map(e => e.message).join("; "));
    }
    return json.data;
  }

  async function getPluginBaseUrl(pluginId) {
    const data = await gql(`
      query Configuration {
        configuration { plugins }
      }
    `);

    const plugins = data?.configuration?.plugins || {};
    const cfg = plugins?.[pluginId] || {};
    return normalizeBaseUrl(cfg.baseUrl);
  }

  async function getSceneUrls(sceneId) {
    const query = `
      query FindSceneUrls($id: ID!) {
        findScene(id: $id) { id urls }
      }
    `;
    try {
      const data = await gql(query, { id: sceneId });
      return data?.findScene?.urls || [];
    } catch {
      // fallback
      const inline = `{ findScene(id: ${sceneId}) { urls } }`;
      const data = await gql(inline, null);
      return data?.findScene?.urls || [];
    }
  }

  function pickMatchingUrl(urls, baseUrl) {
    const base = normalizeBaseUrl(baseUrl);
    if (!base) return null;
    for (const u of urls || []) {
      if (!u) continue;
      if (u.startsWith(base) || u.startsWith(base + "/")) return u;
    }
    return null;
  }

  // --- DOM injection ---------------------------------------------------------

  function removeButtonIfExists() {
    const existing = document.getElementById(HOST_SPAN_ID);
    if (existing) existing.remove();
  }

  function svgFromString(svgStr) {
    const tpl = document.createElement("template");
    tpl.innerHTML = svgStr.trim();
    return tpl.content.firstChild;
  }

  function createIconNode() {
    const img = document.createElement("img");
    img.src = ICON_URL;
    img.alt = "Jellyfin";
    img.decoding = "async";
    img.loading = "eager";
    img.referrerPolicy = "no-referrer";

    //  ак у остальных тулбар-иконок
    img.style.width = "1em";
    img.style.height = "1em";
    img.style.display = "block";

    img.onerror = () => {
      try {
        img.replaceWith(svgFromString(FALLBACK_SVG));
      } catch (e) {
        log("Icon fallback failed:", e);
      }
    };

    return img;
  }

  // Ќаходим span.scene-toolbar-group, где есть "глаз" (счЄтчик воспроизведений)
  function findToolbarGroupWithViews() {
    const groups = Array.from(document.querySelectorAll("span.scene-toolbar-group"));
    for (const g of groups) {
      const eye =
        g.querySelector('div.count-button.increment-only.btn-group svg[data-icon="eye"]') ||
        g.querySelector('div.count-button.increment-only.btn-group .fa-eye') ||
        g.querySelector('div.count-button.increment-only.btn-group button[title*="—четчик"]');
      if (eye) return g;
    }
    return null;
  }

  // ¬ставл¤ем кнопку ¬Ќ”“–№ scene-toolbar-group Ч пр¤мо перед span, в котором находитс¤ div.count-button... с глазом
  function upsertButtonJustBeforeViewsBlock(urlToOpen) {
    const toolbarGroup = findToolbarGroupWithViews();
    if (!toolbarGroup) return false;

    if (!urlToOpen) {
      removeButtonIfExists();
      return true;
    }

    // ќпорный элемент: span-обЄртка, в которой лежит div.count-button... с глазом
    const eyeSvg =
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group svg[data-icon="eye"]') ||
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group .fa-eye') ||
      toolbarGroup.querySelector('div.count-button.increment-only.btn-group button[title*="—четчик"]');

    const refSpan = eyeSvg ? eyeSvg.closest("span") : null;
    if (!refSpan) return false;

    let host = document.getElementById(HOST_SPAN_ID);
    if (!host) {
      // делаем host в формате как остальные элементы внутри toolbarGroup: <span><div role="group" class="btn-group">...</div></span>
      host = document.createElement("span");
      host.id = HOST_SPAN_ID;

      const group = document.createElement("div");
      group.setAttribute("role", "group");
      group.className = "btn-group";

      const btn = document.createElement("button");
      btn.id = BTN_ID;
      btn.type = "button";
      btn.className = "minimal btn btn-secondary";
      btn.title = BTN_TITLE;
      btn.setAttribute("aria-label", BTN_TITLE);

      btn.appendChild(createIconNode());
      group.appendChild(btn);
      host.appendChild(group);

      //  лючевое: вставка ѕ?–?? refSpan (то есть пр¤мо перед блоком с глазом),
      // но уже внутри toolbarGroup Ч поэтому кнопка УприлипаетФ к правой части тулбара.
      toolbarGroup.insertBefore(host, refSpan);
    }

    const btn = document.getElementById(BTN_ID);
    if (btn) {
      btn.onclick = (e) => {
        e.preventDefault();
        e.stopPropagation();
        window.open(urlToOpen, "_blank", "noopener,noreferrer");
      };
      btn.title = `${BTN_TITLE}\n${urlToOpen}`;
    }

    return true;
  }

  async function renderForScene(sceneId) {
    const pluginId = getPluginIdFromScriptUrl();

    let baseUrl = "";
    try {
      baseUrl = await getPluginBaseUrl(pluginId);
    } catch (e) {
      log("Failed to read plugin settings:", e?.message || e);
      baseUrl = "";
    }

    if (!baseUrl) {
      removeButtonIfExists();
      return;
    }

    let urls = [];
    try {
      urls = await getSceneUrls(sceneId);
    } catch (e) {
      log("Failed to read scene urls:", e?.message || e);
      removeButtonIfExists();
      return;
    }

    const match = pickMatchingUrl(urls, baseUrl);
    if (!match) {
      removeButtonIfExists();
      return;
    }

    // React дорисовывает DOM Ч несколько попыток
    let tries = 0;
    const maxTries = 40;
    const tick = () => {
      tries += 1;
      const ok = upsertButtonJustBeforeViewsBlock(match);
      if (!ok && tries < maxTries) setTimeout(tick, 100);
    };
    tick();
  }

  function handleLocation(locationObj) {
    const sceneId = getSceneIdFromLocation(locationObj);
    if (!sceneId) {
      removeButtonIfExists();
      return;
    }
    renderForScene(sceneId);
  }

  // --- init ------------------------------------------------------------------

  handleLocation({ pathname: window.location.pathname });

  if (window.PluginApi?.Event?.addEventListener) {
    PluginApi.Event.addEventListener("stash:location", (e) => {
      const loc = e?.detail?.data?.location;
      handleLocation(loc);
    });
  } else {
    setInterval(() => handleLocation({ pathname: window.location.pathname }), 1000);
  }
})();
