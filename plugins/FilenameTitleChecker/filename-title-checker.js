(function () {
  "use strict";

  const PluginApi = window.PluginApi;
  if (!PluginApi) {
    console.error("[FilenameTitleChecker] PluginApi is not available");
    return;
  }

  const React = PluginApi.React;
  const h = React.createElement;
  const { Link, NavLink } = PluginApi.libraries.ReactRouterDOM;
  const Bootstrap = PluginApi.libraries.Bootstrap || {};
  const Button = Bootstrap.Button || "button";

  const ROUTE = "/plugins/filename-title-checker";
  const DEFAULT_PAGE_SIZE = 50;
  const PAGE_SIZE_OPTIONS = Object.freeze([20, 40, 50, 60, 80, 100]);
  const PAGE_SIZE_STORAGE_KEY = "filename-title-checker.page-size.v1";
  const GRAPHQL_PAGE_SIZE = 500;
  const SAVED_METADATA_STORAGE_KEY = "filename-title-checker.saved-metadata.v1";
  const SOURCE_SELECTION_STORAGE_KEY = "filename-title-checker.graphql-sources.v1";

  // Naming and folder rules copied from the user's attached renamer-dev plugin.
  // The UI plugin intentionally does not read renamer_settings.py at runtime.
  const RENAMER_RULES = Object.freeze({
    separator: " - ",
    noStudioFolder: "No Studio",
    tagSpecificPaths: Object.freeze({
      Movie: "E:\\Movies",
    }),
    studioTemplates: Object.freeze({
      "1By-Day111": Object.freeze(["studio", "date", "performers", "title"]),
    }),
    performerLimit: 3,
  });

  const SCENES_QUERY = `
    query FilenameTitleCheckerScenes($filter: FindFilterType) {
      findScenes(filter: $filter) {
        count
        scenes {
          id
          title
          date
          studio {
            id
            name
          }
          performers {
            id
            name
          }
          tags {
            id
            name
          }
          code
          details
          director
          urls
          stash_ids {
            endpoint
            stash_id
          }
          paths {
            screenshot
          }
          files {
            id
            path
            basename
            height
          }
        }
      }
    }
  `;

  const CONFIG_QUERY = `
    query FilenameTitleCheckerConfig {
      configuration {
        general {
          stashes {
            path
          }
          stashBoxes {
            endpoint
            name
          }
        }
      }
    }
  `;

  const MOVE_FILE_MUTATION = `
    mutation FilenameTitleCheckerMoveFile($input: MoveFilesInput!) {
      moveFiles(input: $input)
    }
  `;

  const SCENE_UPDATE_MUTATION = `
    mutation FilenameTitleCheckerSceneUpdate($input: SceneUpdateInput!) {
      sceneUpdate(input: $input) {
        id
        title
      }
    }
  `;

  const FIND_STUDIO_QUERY = `
    query FilenameTitleCheckerFindStudio($studioFilter: StudioFilterType, $filter: FindFilterType) {
      findStudios(studio_filter: $studioFilter, filter: $filter) {
        studios { id name }
      }
    }
  `;

  const FIND_PERFORMER_QUERY = `
    query FilenameTitleCheckerFindPerformer($performerFilter: PerformerFilterType, $filter: FindFilterType) {
      findPerformers(performer_filter: $performerFilter, filter: $filter) {
        performers { id name }
      }
    }
  `;

  const FIND_TAG_QUERY = `
    query FilenameTitleCheckerFindTag($tagFilter: TagFilterType, $filter: FindFilterType) {
      findTags(tag_filter: $tagFilter, filter: $filter) {
        tags { id name }
      }
    }
  `;

  const STUDIO_CREATE_MUTATION = `
    mutation FilenameTitleCheckerStudioCreate($input: StudioCreateInput!) {
      studioCreate(input: $input) { id name }
    }
  `;

  const PERFORMER_CREATE_MUTATION = `
    mutation FilenameTitleCheckerPerformerCreate($input: PerformerCreateInput!) {
      performerCreate(input: $input) { id name }
    }
  `;

  const TAG_CREATE_MUTATION = `
    mutation FilenameTitleCheckerTagCreate($input: TagCreateInput!) {
      tagCreate(input: $input) { id name }
    }
  `;

  const SCRAPE_SCENE_QUERY = `
    query FilenameTitleCheckerScrapeScene($source: ScraperSourceInput!, $input: ScrapeSingleSceneInput!) {
      scrapeSingleScene(source: $source, input: $input) {
        title
        code
        details
        director
        urls
        date
        image
        remote_site_id
        duration
        studio {
          stored_id
          name
          remote_site_id
          urls
          parent {
            name
          }
        }
        performers {
          stored_id
          name
          remote_site_id
        }
        tags {
          stored_id
          name
          remote_site_id
        }
      }
    }
  `;

  function safeReadLocalStorage(key, fallback) {
    try {
      const raw = window.localStorage.getItem(key);
      if (!raw) return fallback;
      return JSON.parse(raw);
    } catch (err) {
      console.warn("[FilenameTitleChecker] Failed to read localStorage", key, err);
      return fallback;
    }
  }

  function safeWriteLocalStorage(key, value) {
    try {
      window.localStorage.setItem(key, JSON.stringify(value));
      return true;
    } catch (err) {
      console.warn("[FilenameTitleChecker] Failed to write localStorage", key, err);
      return false;
    }
  }

  function compactScrapedCandidate(candidate, source) {
    return {
      sourceName: source.name || source.endpoint || "GraphQL",
      sourceEndpoint: source.endpoint || "",
      remoteSiteId: candidate.remote_site_id || "",
      title: String(candidate.title || "").trim(),
      date: String(candidate.date || "").trim(),
      image: String(candidate.image || "").trim(),
      studio: candidate.studio && candidate.studio.name ? String(candidate.studio.name).trim() : "",
      studioStoredId: candidate.studio && candidate.studio.stored_id ? String(candidate.studio.stored_id) : "",
      studioRemoteSiteId: candidate.studio && candidate.studio.remote_site_id ? String(candidate.studio.remote_site_id) : "",
      performers: (candidate.performers || []).map((performer) => performer.name).filter(Boolean),
      performerEntities: (candidate.performers || []).filter((performer) => performer && performer.name).map((performer) => ({
        name: String(performer.name).trim(),
        storedId: performer.stored_id ? String(performer.stored_id) : "",
        remoteSiteId: performer.remote_site_id ? String(performer.remote_site_id) : "",
      })),
      tags: (candidate.tags || []).map((tag) => tag.name).filter(Boolean),
      tagEntities: (candidate.tags || []).filter((tag) => tag && tag.name).map((tag) => ({
        name: String(tag.name).trim(),
        storedId: tag.stored_id ? String(tag.stored_id) : "",
        remoteSiteId: tag.remote_site_id ? String(tag.remote_site_id) : "",
      })),
      code: String(candidate.code || "").trim(),
      director: String(candidate.director || "").trim(),
      urls: (candidate.urls || []).filter(Boolean),
      duration: candidate.duration || null,
      details: String(candidate.details || "").trim(),
    };
  }

  function candidateIdentity(candidate) {
    return [
      candidate.sourceEndpoint || "",
      candidate.remoteSiteId || "",
      normalizedText(candidate.title || ""),
      normalizedText(candidate.studio || ""),
      candidate.date || "",
      normalizedText(candidate.code || ""),
    ].join("|");
  }

  function applyMetadataOverride(item, savedMetadata) {
    if (!savedMetadata) return item;
    return {
      ...item,
      title: savedMetadata.title || item.title,
      date: savedMetadata.date || item.date,
      studio: savedMetadata.studio || item.studio,
      performers: savedMetadata.performers && savedMetadata.performers.length ? savedMetadata.performers : item.performers,
    };
  }

  function remoteSceneUrl(endpoint, remoteSiteId) {
    if (!endpoint || !remoteSiteId) return "";
    try {
      const url = new URL(endpoint, window.location.origin);
      url.pathname = url.pathname.replace(/\/graphql\/?$/u, "").replace(/\/$/u, "") + `/scenes/${remoteSiteId}`;
      url.search = "";
      url.hash = "";
      return url.toString();
    } catch (_err) {
      return "";
    }
  }

  function basenameFromPath(path) {
    if (!path) return "";
    const parts = String(path).split(/[\\/]/);
    return parts[parts.length - 1] || "";
  }

  function stripExtension(filename) {
    const name = String(filename || "");
    const lastDot = name.lastIndexOf(".");
    if (lastDot <= 0) return name;
    return name.slice(0, lastDot);
  }

  function extensionFromFilename(filename) {
    const name = String(filename || "");
    const lastDot = name.lastIndexOf(".");
    if (lastDot <= 0 || lastDot === name.length - 1) return "";
    return name.slice(lastDot);
  }

  function canonicalizeTokens(tokens) {
    // No spelling-specific or split/join aliases are applied here.
    // Matching remains strict after the general normalization rules below.
    return tokens;
  }

  function normalizeTokens(value) {
    const tokens = String(value || "")
      .replace(/[’'`´ʼʻʹ＇]/gu, "")
      .normalize("NFKD")
      .replace(/\p{M}+/gu, "")
      .toLocaleLowerCase()
      .replace(/&/gu, " and ")
      .replace(/([\p{L}])([\p{N}])/gu, "$1 $2")
      .replace(/([\p{N}])([\p{L}])/gu, "$1 $2")
      .replace(/[\p{P}\p{S}_]+/gu, " ")
      .replace(/\s+/gu, " ")
      .trim()
      .split(" ")
      .filter(Boolean);

    return canonicalizeTokens(tokens);
  }

  function containsTokenSequence(filename, title) {
    const fileTokens = normalizeTokens(stripExtension(filename));
    const titleTokens = normalizeTokens(title);

    if (!titleTokens.length || !fileTokens.length || titleTokens.length > fileTokens.length) {
      return false;
    }

    outer: for (let i = 0; i <= fileTokens.length - titleTokens.length; i += 1) {
      for (let j = 0; j < titleTokens.length; j += 1) {
        if (fileTokens[i + j] !== titleTokens[j]) {
          continue outer;
        }
      }
      return true;
    }

    return false;
  }

  function normalizedText(value) {
    return normalizeTokens(value).join(" ");
  }

  function normalizedStudioKey(value) {
    // Studio names often differ only in spacing/capitalization between sources
    // and folder names (for example "AcademyPOV" vs "Academy POV").
    // Reuse the safe general normalization, then remove token boundaries.
    return normalizeTokens(value).join("");
  }

  function studioNamesEquivalent(a, b) {
    const left = normalizedStudioKey(a);
    const right = normalizedStudioKey(b);
    return Boolean(left && right) && left === right;
  }

  function performerSetSignature(performers) {
    const names = Array.from(new Set(
      (performers || [])
        .map((name) => normalizedText(name))
        .filter(Boolean)
    )).sort();
    return names.length ? names.join("|") : "";
  }

  function sharedPerformerSignatures(sourceResults) {
    const sourcesBySignature = new Map();

    (sourceResults || []).forEach((sourceResult) => {
      if (!sourceResult || sourceResult.status !== "ok") return;
      const sourceKey = sourceResult.endpoint || sourceResult.name || "source";
      const seenInThisSource = new Set();

      (sourceResult.candidates || []).forEach((candidate) => {
        const signature = performerSetSignature(candidate.performers);
        if (signature) seenInThisSource.add(signature);
      });

      seenInThisSource.forEach((signature) => {
        if (!sourcesBySignature.has(signature)) sourcesBySignature.set(signature, new Set());
        sourcesBySignature.get(signature).add(sourceKey);
      });
    });

    const shared = new Set();
    sourcesBySignature.forEach((sources, signature) => {
      if (sources.size >= 2) shared.add(signature);
    });
    return shared;
  }

  function extractMetadataFromFilename(filename) {
    const name = stripExtension(filename);
    const parts = String(name || "")
      .split(/\s+-\s+/u)
      .map((part) => part.trim())
      .filter(Boolean);

    const empty = { parsed: false, date: "", studio: "", title: "" };
    if (parts.length < 2) return empty;

    const datePattern = /^\d{4}-\d{2}-\d{2}$/u;
    const qualityPattern = /^\[[^\]]+\]$/u;

    // Default template: Date - Studio - Title - [Quality].
    // Title itself may contain " - ", so keep every segment between Studio
    // and the trailing quality marker.
    if (datePattern.test(parts[0])) {
      const end = parts.length > 3 && qualityPattern.test(parts[parts.length - 1]) ? parts.length - 1 : parts.length;
      return {
        parsed: true,
        date: parts[0] || "",
        studio: parts[1] || "",
        title: parts.slice(2, end).join(" - ").trim(),
      };
    }

    // Special template used by 1By-Day111: Studio - Date - Performers - Title.
    // Its generated Title is the final segment.
    if (datePattern.test(parts[1])) {
      return {
        parsed: true,
        date: parts[1] || "",
        studio: parts[0] || "",
        title: parts.length >= 4 ? parts.slice(3).join(" - ").trim() : "",
      };
    }

    return empty;
  }

  function extractStudioFromFilename(filename) {
    return extractMetadataFromFilename(filename).studio;
  }

  function studioComparison(filename, sceneStudio) {
    const fileStudio = extractStudioFromFilename(filename);
    if (!fileStudio) {
      return { parsed: false, fileStudio: "", matches: false, differs: false };
    }

    const matches = studioNamesEquivalent(fileStudio, sceneStudio);
    const differs = !matches;

    return { parsed: true, fileStudio, matches, differs };
  }

  function studioNameForFilename(item) {
    const currentFileStudio = extractStudioFromFilename(item && item.filename ? item.filename : "");
    const desiredStudio = String(item && item.studio ? item.studio : "").trim() || currentFileStudio;
    if (!desiredStudio) return "";

    // Preserve the spelling/spacing already used in the filename when it is
    // equivalent to the current/selected Studio. This avoids cosmetic renames
    // such as "5K Teens" -> "5Kteens" or "Academy POV" -> "AcademyPOV".
    // If the Scene currently has no Studio, the parsed Studio from the existing
    // filename is a safe fallback until GraphQL metadata is applied.
    if (currentFileStudio && studioNamesEquivalent(currentFileStudio, desiredStudio)) {
      return currentFileStudio;
    }

    return desiredStudio;
  }

  async function graphqlRequest(query, variables, signal) {
    const response = await fetch("/graphql", {
      method: "POST",
      credentials: "same-origin",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify({ query, variables }),
      signal,
    });

    if (!response.ok) {
      throw new Error(`GraphQL HTTP error ${response.status}: ${response.statusText}`);
    }

    const payload = await response.json();
    if (payload.errors && payload.errors.length) {
      throw new Error(payload.errors.map((item) => item.message).join("; "));
    }

    return payload.data;
  }

  function stashIdInput(endpoint, remoteSiteId) {
    if (!endpoint || !remoteSiteId) return [];
    return [{ endpoint, stash_id: remoteSiteId }];
  }

  async function resolveOrCreateStudio(entity, sourceEndpoint) {
    if (!entity || !entity.name) return "";
    if (entity.storedId) return entity.storedId;
    const found = await graphqlRequest(FIND_STUDIO_QUERY, {
      studioFilter: { name: { value: entity.name, modifier: "EQUALS" } },
      filter: { per_page: 10 },
    });
    const matches = found && found.findStudios ? found.findStudios.studios || [] : [];
    const exact = matches.find((item) => normalizedText(item.name) === normalizedText(entity.name));
    if (exact) return String(exact.id);
    const created = await graphqlRequest(STUDIO_CREATE_MUTATION, {
      input: { name: entity.name, stash_ids: stashIdInput(sourceEndpoint, entity.remoteSiteId) },
    });
    return created && created.studioCreate ? String(created.studioCreate.id) : "";
  }

  async function resolveOrCreatePerformer(entity, sourceEndpoint) {
    if (!entity || !entity.name) return "";
    if (entity.storedId) return entity.storedId;
    const found = await graphqlRequest(FIND_PERFORMER_QUERY, {
      performerFilter: { name: { value: entity.name, modifier: "EQUALS" } },
      filter: { per_page: 10 },
    });
    const matches = found && found.findPerformers ? found.findPerformers.performers || [] : [];
    const exact = matches.find((item) => normalizedText(item.name) === normalizedText(entity.name));
    if (exact) return String(exact.id);
    const created = await graphqlRequest(PERFORMER_CREATE_MUTATION, {
      input: { name: entity.name, stash_ids: stashIdInput(sourceEndpoint, entity.remoteSiteId) },
    });
    return created && created.performerCreate ? String(created.performerCreate.id) : "";
  }

  async function resolveOrCreateTag(entity, sourceEndpoint) {
    if (!entity || !entity.name) return "";
    if (entity.storedId) return entity.storedId;
    const found = await graphqlRequest(FIND_TAG_QUERY, {
      tagFilter: { name: { value: entity.name, modifier: "EQUALS" } },
      filter: { per_page: 10 },
    });
    const matches = found && found.findTags ? found.findTags.tags || [] : [];
    const exact = matches.find((item) => normalizedText(item.name) === normalizedText(entity.name));
    if (exact) return String(exact.id);
    const created = await graphqlRequest(TAG_CREATE_MUTATION, {
      input: { name: entity.name, stash_ids: stashIdInput(sourceEndpoint, entity.remoteSiteId) },
    });
    return created && created.tagCreate ? String(created.tagCreate.id) : "";
  }

  function mergeUniqueStrings(a, b) {
    const seen = new Set();
    const out = [];
    for (const value of [...(a || []), ...(b || [])]) {
      const text = String(value || "").trim();
      if (!text || seen.has(text)) continue;
      seen.add(text);
      out.push(text);
    }
    return out;
  }

  function canonicalStashEndpoint(value) {
    return String(value || "").trim().replace(/\/+$/, "").toLocaleLowerCase();
  }

  function mergeStashIds(existing, endpoint, remoteSiteId) {
    // Stash enforces one Stash ID per Scene + endpoint. When a GraphQL source
    // returns a different remote scene ID for an endpoint that is already
    // attached to the Scene, replace that endpoint's ID instead of appending a
    // second row (which would violate scene_stash_ids(scene_id, endpoint)).
    const targetEndpoint = String(endpoint || "").trim();
    const targetStashId = String(remoteSiteId || "").trim();
    const targetKey = canonicalStashEndpoint(targetEndpoint);
    const seenEndpoints = new Set();
    const values = [];

    for (const item of existing || []) {
      const itemEndpoint = String(item && item.endpoint || "").trim();
      const itemStashId = String(item && item.stash_id || "").trim();
      if (!itemEndpoint || !itemStashId) continue;

      const itemKey = canonicalStashEndpoint(itemEndpoint);
      if (!itemKey || itemKey === targetKey || seenEndpoints.has(itemKey)) continue;

      seenEndpoints.add(itemKey);
      values.push({ endpoint: itemEndpoint, stash_id: itemStashId });
    }

    if (targetEndpoint && targetStashId) {
      values.push({ endpoint: targetEndpoint, stash_id: targetStashId });
    }

    return values;
  }

  function replaceIllegalCharacters(value) {
    // Same character replacement used by the attached renamer-dev plugin.
    return String(value || "").replace(/[<>:"/\\|?*]/g, "-");
  }

  function normalizeFsPath(path) {
    let value = String(path || "").replace(/\\/g, "/").replace(/\/+$/g, "");
    // Windows paths are case-insensitive. Lowercasing all paths is harmless for
    // prefix matching because we only use this representation for comparison.
    return value.toLocaleLowerCase();
  }

  function pathIsInside(path, root) {
    const normalizedPath = normalizeFsPath(path);
    const normalizedRoot = normalizeFsPath(root);
    if (!normalizedPath || !normalizedRoot) return false;
    return normalizedPath === normalizedRoot || normalizedPath.startsWith(`${normalizedRoot}/`);
  }

  function findContainingStash(path, stashPaths) {
    const candidates = (stashPaths || [])
      .filter((root) => pathIsInside(path, root))
      .sort((a, b) => normalizeFsPath(b).length - normalizeFsPath(a).length);
    return candidates[0] || "";
  }

  function findConfiguredPath(path, stashPaths) {
    const normalized = normalizeFsPath(path);
    return (stashPaths || []).find((root) => normalizeFsPath(root) === normalized) || "";
  }

  function preferredSeparator(path) {
    const value = String(path || "");
    return value.includes("\\") && !value.includes("/") ? "\\" : "/";
  }

  function joinFsPath(root, child) {
    const separator = preferredSeparator(root);
    const cleanRoot = String(root || "").replace(/[\\/]+$/g, "");
    const cleanChild = String(child || "").replace(/^[\\/]+|[\\/]+$/g, "");
    return cleanChild ? `${cleanRoot}${separator}${cleanChild}` : cleanRoot;
  }

  function sameFsPath(a, b) {
    return normalizeFsPath(a) === normalizeFsPath(b);
  }

  function dirnameFromPath(path) {
    const value = String(path || "");
    const slash = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
    return slash >= 0 ? value.slice(0, slash) : "";
  }

  function buildDefaultFilenameParts(item) {
    const parts = [];
    const filenameStudio = studioNameForFilename(item);
    if (item.date) parts.push(replaceIllegalCharacters(item.date));
    if (filenameStudio) parts.push(replaceIllegalCharacters(filenameStudio));
    if (item.title) parts.push(replaceIllegalCharacters(item.title));
    if (item.height) parts.push(`[WEBDL-${replaceIllegalCharacters(String(item.height))}p]`);
    return parts;
  }

  function buildStudioTemplateParts(item) {
    const template = RENAMER_RULES.studioTemplates[item.studio];
    if (!template) return null;

    const values = {
      studio: studioNameForFilename(item),
      date: item.date,
      title: item.title,
      performers: (item.performers || [])
        .slice()
        .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }))
        .slice(0, RENAMER_RULES.performerLimit)
        .join(RENAMER_RULES.separator),
    };

    return template
      .map((key) => replaceIllegalCharacters(values[key] || ""))
      .filter(Boolean);
  }

  function buildTargetBasename(item) {
    const extension = extensionFromFilename(item.filename);
    if (!extension) {
      return { ok: false, error: "Не удалось определить расширение файла" };
    }
    if (!String(item.title || "").trim()) {
      return { ok: false, error: "У сцены нет Title" };
    }

    const parts = buildStudioTemplateParts(item) || buildDefaultFilenameParts(item);
    if (!parts.length) {
      return { ok: false, error: "Недостаточно данных для нового имени" };
    }

    return {
      ok: true,
      basename: `${parts.join(RENAMER_RULES.separator)}${extension}`,
    };
  }

  function buildRenamePlan(item, stashPaths) {
    if (!item.fileId || !item.path) {
      return { ok: false, error: "У записи нет видеофайла" };
    }

    const basenameResult = buildTargetBasename(item);
    if (!basenameResult.ok) return basenameResult;

    const tags = new Set(item.tags || []);
    let targetRoot = "";
    let specialRoot = "";

    for (const [tag, configuredPath] of Object.entries(RENAMER_RULES.tagSpecificPaths)) {
      if (tags.has(tag)) {
        specialRoot = configuredPath;
        const containingLibrary = findContainingStash(configuredPath, stashPaths);
        if (!containingLibrary) {
          return {
            ok: false,
            error: `Для тега ${tag} задан путь ${configuredPath}, но он не входит ни в один Stash Library Path`,
          };
        }
        targetRoot = configuredPath;
        break;
      }
    }

    if (!targetRoot) {
      targetRoot = findContainingStash(item.path, stashPaths);
    }

    if (!targetRoot) {
      return { ok: false, error: "Не удалось определить корневой Stash Library Path для файла" };
    }

    const sourceFolder = dirnameFromPath(item.path);
    const sourceFolderName = basenameFromPath(sourceFolder);
    // Priority for the destination Studio is:
    // selected/saved GraphQL Studio (already applied to item.studio) -> current
    // Scene Studio -> Studio parsed from the current filename -> No Studio.
    // This prevents an untagged Scene from being previewed/moved to "No Studio"
    // when its existing filename already carries a valid Studio name.
    const parsedFileStudio = extractStudioFromFilename(item.filename || basenameFromPath(item.path));
    const desiredStudioName = String(item.studio || "").trim() || parsedFileStudio || RENAMER_RULES.noStudioFolder;
    const studioFolder = replaceIllegalCharacters(desiredStudioName);

    // Do not move a file just to change harmless Studio folder formatting.
    // Examples considered equivalent: 5K Teens/5Kteens, Academy POV/AcademyPOV,
    // Free Use MILF/Freeuse MILF, Got Mylf/Got MYLF.
    const keepCurrentStudioFolder =
      pathIsInside(sourceFolder, targetRoot) && studioNamesEquivalent(sourceFolderName, desiredStudioName);
    const targetFolder = keepCurrentStudioFolder ? sourceFolder : joinFsPath(targetRoot, studioFolder);
    const targetPath = joinFsPath(targetFolder, basenameResult.basename);

    return {
      ok: true,
      fileId: item.fileId,
      sourcePath: item.path,
      sourceFolder,
      destinationFolder: targetFolder,
      destinationBasename: basenameResult.basename,
      destinationPath: targetPath,
      moveNeeded: !sameFsPath(sourceFolder, targetFolder),
      renameNeeded: basenameFromPath(item.path) !== basenameResult.basename,
      specialRoot,
    };
  }

  function buildIssue(scene, file, reason, studioInfo) {
    const filename = file ? (file.basename || basenameFromPath(file.path)) : "";
    const comparison = studioInfo || { parsed: false, fileStudio: "", matches: false, differs: false };
    return {
      key: `${scene.id}:${file ? file.id : reason}`,
      sceneId: scene.id,
      title: scene.title || "",
      date: scene.date || "",
      studio: scene.studio ? scene.studio.name : "",
      studioId: scene.studio ? scene.studio.id : "",
      performers: (scene.performers || []).map((performer) => performer.name).filter(Boolean),
      performerIds: (scene.performers || []).map((performer) => String(performer.id)).filter(Boolean),
      tags: (scene.tags || []).map((tag) => tag.name).filter(Boolean),
      tagIds: (scene.tags || []).map((tag) => String(tag.id)).filter(Boolean),
      code: scene.code || "",
      details: scene.details || "",
      director: scene.director || "",
      urls: (scene.urls || []).filter(Boolean),
      stashIds: (scene.stash_ids || []).map((stashId) => ({ endpoint: stashId.endpoint, stash_id: stashId.stash_id })),
      screenshot: scene.paths ? scene.paths.screenshot : "",
      fileId: file ? file.id : "",
      filename,
      path: file ? file.path || "" : "",
      height: file && file.height ? file.height : null,
      reason,
      filenameStudio: comparison.fileStudio || "",
      studioParsedFromFilename: Boolean(comparison.parsed),
      studioMatchesFilename: Boolean(comparison.matches),
      studioDiffersFilename: Boolean(comparison.differs),
    };
  }

  function reasonText(reason) {
    if (reason === "no_title") return "У сцены не задан Title";
    if (reason === "no_files") return "У сцены нет видеофайла";
    return "Title не найден в имени файла";
  }

  function reasonClass(reason) {
    if (reason === "no_title") return "ftc-status ftc-status-warning";
    if (reason === "no_files") return "ftc-status ftc-status-muted";
    return "ftc-status ftc-status-error";
  }

  function SummaryCard(props) {
    return h(
      "div",
      { className: "ftc-summary-card" },
      h("div", { className: "ftc-summary-value" }, String(props.value)),
      h("div", { className: "ftc-summary-label" }, props.label)
    );
  }

  function FilenameTitleCheckerPage() {
    const [issues, setIssues] = React.useState([]);
    const [stashPaths, setStashPaths] = React.useState([]);
    const [stashBoxes, setStashBoxes] = React.useState([]);
    const [selectedSourceEndpoints, setSelectedSourceEndpoints] = React.useState(function () {
      const stored = safeReadLocalStorage(SOURCE_SELECTION_STORAGE_KEY, []);
      return new Set(Array.isArray(stored) ? stored : []);
    });
    const [lookupResults, setLookupResults] = React.useState({});
    const [lookupLoading, setLookupLoading] = React.useState(new Set());
    const [lookupBatchState, setLookupBatchState] = React.useState({ running: false, done: 0, total: 0 });
    const [savedMetadata, setSavedMetadata] = React.useState(function () {
      const stored = safeReadLocalStorage(SAVED_METADATA_STORAGE_KEY, {});
      return stored && typeof stored === "object" && !Array.isArray(stored) ? stored : {};
    });
    const [stats, setStats] = React.useState({
      scenes: 0,
      files: 0,
      matches: 0,
      mismatches: 0,
      noTitle: 0,
      noFiles: 0,
    });
    const [loading, setLoading] = React.useState(false);
    const [progress, setProgress] = React.useState({ checked: 0, total: 0 });
    const [error, setError] = React.useState("");
    const [search, setSearch] = React.useState("");
    const [studio, setStudio] = React.useState("all");
    const [reason, setReason] = React.useState("all");
    const [sameStudioOnly, setSameStudioOnly] = React.useState(false);
    const [differentStudioOnly, setDifferentStudioOnly] = React.useState(false);
    const [page, setPage] = React.useState(1);
    const [pageSize, setPageSize] = React.useState(function () {
      const saved = Number(safeReadLocalStorage(PAGE_SIZE_STORAGE_KEY, DEFAULT_PAGE_SIZE));
      return PAGE_SIZE_OPTIONS.includes(saved) ? saved : DEFAULT_PAGE_SIZE;
    });
    const [selectedKeys, setSelectedKeys] = React.useState(new Set());
    const [renameState, setRenameState] = React.useState({ running: false, done: 0, total: 0 });
    const [metadataSaving, setMetadataSaving] = React.useState(new Set());
    const [operationMessage, setOperationMessage] = React.useState(null);
    const abortRef = React.useRef(null);

    const scan = React.useCallback(async function scanLibrary() {
      if (abortRef.current) {
        abortRef.current.abort();
      }

      const controller = new AbortController();
      abortRef.current = controller;

      setLoading(true);
      setError("");
      setIssues([]);
      setSelectedKeys(new Set());
      setPage(1);
      setProgress({ checked: 0, total: 0 });

      const nextIssues = [];
      const nextStats = {
        scenes: 0,
        files: 0,
        matches: 0,
        mismatches: 0,
        noTitle: 0,
        noFiles: 0,
      };

      try {
        const configData = await graphqlRequest(CONFIG_QUERY, {}, controller.signal);
        const configStashes = configData && configData.configuration && configData.configuration.general
          ? configData.configuration.general.stashes || []
          : [];
        const nextStashPaths = configStashes.map((stash) => stash.path).filter(Boolean);
        setStashPaths(nextStashPaths);
        const configBoxes = configData && configData.configuration && configData.configuration.general
          ? configData.configuration.general.stashBoxes || []
          : [];
        const nextBoxes = configBoxes
          .map((box, index) => ({ endpoint: box.endpoint || "", name: box.name || box.endpoint || `GraphQL ${index + 1}` }))
          .filter((box) => box.endpoint);
        setStashBoxes(nextBoxes);
        setSelectedSourceEndpoints((previous) => {
          const valid = new Set(nextBoxes.map((box) => box.endpoint));
          const retained = new Set(Array.from(previous).filter((endpoint) => valid.has(endpoint)));
          const next = retained.size ? retained : new Set(valid);
          safeWriteLocalStorage(SOURCE_SELECTION_STORAGE_KEY, Array.from(next));
          return next;
        });

        let graphqlPage = 1;
        let totalScenes = null;

        while (totalScenes === null || nextStats.scenes < totalScenes) {
          const data = await graphqlRequest(
            SCENES_QUERY,
            {
              filter: {
                page: graphqlPage,
                per_page: GRAPHQL_PAGE_SIZE,
              },
            },
            controller.signal
          );

          const result = data && data.findScenes;
          if (!result) {
            throw new Error("Stash did not return findScenes data");
          }

          if (totalScenes === null) {
            totalScenes = result.count || 0;
            setProgress({ checked: 0, total: totalScenes });
          }

          const scenes = result.scenes || [];
          if (!scenes.length) break;

          for (const scene of scenes) {
            nextStats.scenes += 1;
            const title = String(scene.title || "").trim();
            const files = scene.files || [];

            if (!files.length) {
              nextStats.noFiles += 1;
              nextIssues.push(buildIssue(scene, null, "no_files", null));
              continue;
            }

            nextStats.files += files.length;

            if (!title) {
              nextStats.noTitle += 1;
              nextStats.mismatches += files.length;
              for (const file of files) {
                nextIssues.push(buildIssue(scene, file, "no_title", studioComparison(file.basename || basenameFromPath(file.path), scene.studio ? scene.studio.name : "")));
              }
              continue;
            }

            for (const file of files) {
              const filename = file.basename || basenameFromPath(file.path);
              if (containsTokenSequence(filename, title)) {
                nextStats.matches += 1;
              } else {
                nextStats.mismatches += 1;
                const studioName = scene.studio ? String(scene.studio.name || "").trim() : "";
                const comparison = studioComparison(filename, studioName);
                nextIssues.push(buildIssue(scene, file, "mismatch", comparison));
              }
            }
          }

          setProgress({ checked: nextStats.scenes, total: totalScenes || 0 });
          graphqlPage += 1;

          if (nextStats.scenes >= totalScenes) break;
        }

        nextIssues.sort(function (a, b) {
          const studioCompare = (a.studio || "").localeCompare(b.studio || "", undefined, { sensitivity: "base" });
          if (studioCompare !== 0) return studioCompare;
          return (a.title || a.filename).localeCompare(b.title || b.filename, undefined, { sensitivity: "base" });
        });

        setStats(nextStats);
        setIssues(nextIssues);
      } catch (err) {
        if (err && err.name !== "AbortError") {
          console.error("[FilenameTitleChecker] Scan failed", err);
          setError(err && err.message ? err.message : String(err));
        }
      } finally {
        if (abortRef.current === controller) {
          abortRef.current = null;
          setLoading(false);
        }
      }
    }, []);

    React.useEffect(function () {
      scan();
      return function cleanup() {
        if (abortRef.current) abortRef.current.abort();
      };
    }, [scan]);

    const studios = React.useMemo(function () {
      const values = new Set();
      for (const item of issues) {
        if (item.studio) values.add(item.studio);
      }
      return Array.from(values).sort(function (a, b) {
        return a.localeCompare(b, undefined, { sensitivity: "base" });
      });
    }, [issues]);

    const filteredIssues = React.useMemo(function () {
      const q = search.trim().toLocaleLowerCase();
      return issues.filter(function (item) {
        if (studio !== "all" && item.studio !== studio) return false;
        if (reason !== "all" && item.reason !== reason) return false;
        if (sameStudioOnly && !(item.reason === "mismatch" && item.studioMatchesFilename)) return false;
        if (differentStudioOnly && !(item.reason === "mismatch" && item.studioDiffersFilename)) return false;
        if (!q) return true;

        return [item.title, item.filename, item.path, item.studio]
          .join("\n")
          .toLocaleLowerCase()
          .includes(q);
      });
    }, [issues, search, studio, reason, sameStudioOnly, differentStudioOnly]);

    React.useEffect(function () {
      setPage(1);
    }, [search, studio, reason, sameStudioOnly, differentStudioOnly, pageSize]);

    const selectedItems = React.useMemo(function () {
      return issues.filter((item) => selectedKeys.has(item.key));
    }, [issues, selectedKeys]);

    const selectedPlans = React.useMemo(function () {
      return selectedItems.map((item) => ({ item, plan: buildRenamePlan(applyMetadataOverride(item, savedMetadata[item.sceneId]), stashPaths) }));
    }, [selectedItems, stashPaths, savedMetadata]);

    const pageCount = Math.max(1, Math.ceil(filteredIssues.length / pageSize));
    const safePage = Math.min(page, pageCount);
    const pageItems = filteredIssues.slice((safePage - 1) * pageSize, safePage * pageSize);
    const eligiblePageItems = pageItems.filter((item) => buildRenamePlan(applyMetadataOverride(item, savedMetadata[item.sceneId]), stashPaths).ok);
    const selectedPageItems = pageItems.filter((item) => selectedKeys.has(item.key));
    const allPageSelected = eligiblePageItems.length > 0 && eligiblePageItems.every((item) => selectedKeys.has(item.key));

    function toggleSelection(item) {
      const plan = buildRenamePlan(applyMetadataOverride(item, savedMetadata[item.sceneId]), stashPaths);
      if (!plan.ok || renameState.running) return;
      setSelectedKeys((previous) => {
        const next = new Set(previous);
        if (next.has(item.key)) next.delete(item.key);
        else next.add(item.key);
        return next;
      });
    }

    function toggleAllOnPage() {
      if (renameState.running || eligiblePageItems.length === 0) return;
      setSelectedKeys((previous) => {
        const next = new Set(previous);
        if (allPageSelected) {
          eligiblePageItems.forEach((item) => next.delete(item.key));
        } else {
          eligiblePageItems.forEach((item) => next.add(item.key));
        }
        return next;
      });
    }

    function toggleSource(endpoint) {
      setSelectedSourceEndpoints((previous) => {
        const next = new Set(previous);
        if (next.has(endpoint)) next.delete(endpoint);
        else next.add(endpoint);
        safeWriteLocalStorage(SOURCE_SELECTION_STORAGE_KEY, Array.from(next));
        return next;
      });
    }

    async function lookupScene(item) {
      const sources = stashBoxes.filter((box) => selectedSourceEndpoints.has(box.endpoint));
      if (!sources.length) {
        setOperationMessage({ type: "warning", text: "Выберите хотя бы один GraphQL-источник." });
        return;
      }

      const sceneId = item.sceneId;
      setLookupLoading((previous) => { const next = new Set(previous); next.add(sceneId); return next; });
      const sourceResults = [];

      for (const source of sources) {
        try {
          const data = await graphqlRequest(SCRAPE_SCENE_QUERY, {
            source: { stash_box_endpoint: source.endpoint },
            input: { scene_id: sceneId },
          });
          const candidates = ((data && data.scrapeSingleScene) || []).map((candidate) => compactScrapedCandidate(candidate, source));
          sourceResults.push({
            endpoint: source.endpoint,
            name: source.name,
            status: "ok",
            candidates,
          });
        } catch (err) {
          console.error("[FilenameTitleChecker] GraphQL source lookup failed", source.endpoint, err);
          sourceResults.push({
            endpoint: source.endpoint,
            name: source.name,
            status: "error",
            error: err && err.message ? err.message : String(err),
            candidates: [],
          });
        }
      }

      setLookupResults((previous) => ({ ...previous, [sceneId]: sourceResults }));
      setLookupLoading((previous) => { const next = new Set(previous); next.delete(sceneId); return next; });

      // If this row was selected for a GraphQL lookup, clear its checkbox after
      // the lookup finishes. This keeps the rename selection from being carried
      // over accidentally to the next operation.
      setSelectedKeys((previous) => {
        if (!previous.has(item.key)) return previous;
        const next = new Set(previous);
        next.delete(item.key);
        return next;
      });
    }

    async function lookupSelected() {
      if (lookupBatchState.running || renameState.running) return;
      const unique = [];
      const seen = new Set();
      for (const item of selectedItems) {
        if (!seen.has(item.sceneId)) { seen.add(item.sceneId); unique.push(item); }
      }
      if (!unique.length) return;
      if (!Array.from(selectedSourceEndpoints).length) {
        setOperationMessage({ type: "warning", text: "Выберите хотя бы один GraphQL-источник." });
        return;
      }
      const requestedItems = selectedItems.slice();
      setLookupBatchState({ running: true, done: 0, total: unique.length });
      for (let index = 0; index < unique.length; index += 1) {
        await lookupScene(unique[index]);
        setLookupBatchState({ running: true, done: index + 1, total: unique.length });
      }

      // A Scene can theoretically have more than one selected video file while
      // the metadata request itself runs once per Scene. Clear every checkbox
      // that participated in this batch, not only the representative row.
      setSelectedKeys((previous) => {
        const next = new Set(previous);
        requestedItems.forEach((item) => next.delete(item.key));
        return next;
      });
      setLookupBatchState({ running: false, done: unique.length, total: unique.length });
    }

    async function saveCandidate(sceneId, candidate, item, options) {
      if (metadataSaving.has(sceneId) || renameState.running) return;
      setMetadataSaving((previous) => { const next = new Set(previous); next.add(sceneId); return next; });

      const saveOptions = options || {};
      const useFilenameTitle = Boolean(saveOptions.useFilenameTitle);
      const filenameMetadata = extractMetadataFromFilename(item.filename || basenameFromPath(item.path));
      const filenameTitle = String(filenameMetadata.title || "").trim();

      if (useFilenameTitle && !filenameTitle) {
        setOperationMessage({
          type: "warning",
          text: `Не удалось определить Title из имени файла ${item.filename || item.path || ""}. Данные не сохранены.`,
        });
        setMetadataSaving((previous) => { const next = new Set(previous); next.delete(sceneId); return next; });
        return;
      }

      const titleToSave = useFilenameTitle ? filenameTitle : String(candidate.title || "").trim();
      setOperationMessage({
        type: "info",
        text: useFilenameTitle
          ? `Сохраняются данные ${candidate.sourceName || "GraphQL"} в Scene ${sceneId}; Title будет взят из имени файла…`
          : `Сохраняются данные ${candidate.sourceName || "GraphQL"} в Scene ${sceneId}…`,
      });

      try {
        const input = { id: sceneId };
        if (titleToSave) input.title = titleToSave;
        if (candidate.date) input.date = candidate.date;
        if (candidate.code) input.code = candidate.code;
        if (candidate.details) input.details = candidate.details;
        if (candidate.director) input.director = candidate.director;
        if (candidate.urls && candidate.urls.length) input.urls = mergeUniqueStrings(item.urls || [], candidate.urls);
        if (candidate.image) input.cover_image = candidate.image;

        if (candidate.studio) {
          // Reuse the current local Studio when only harmless formatting differs
          // (Academy POV/AcademyPOV, 5K Teens/5Kteens, etc.) to avoid creating
          // duplicate Studio records merely because another source uses spacing/case differently.
          const equivalentCurrentStudioId = item.studioId && studioNamesEquivalent(candidate.studio, item.studio)
            ? String(item.studioId)
            : "";
          const studioId = equivalentCurrentStudioId || await resolveOrCreateStudio({
            name: candidate.studio,
            storedId: candidate.studioStoredId,
            remoteSiteId: candidate.studioRemoteSiteId,
          }, candidate.sourceEndpoint);
          if (studioId) input.studio_id = studioId;
        }

        if (candidate.performerEntities && candidate.performerEntities.length) {
          const performerIds = [];
          for (const performer of candidate.performerEntities) {
            const performerId = await resolveOrCreatePerformer(performer, candidate.sourceEndpoint);
            if (performerId) performerIds.push(performerId);
          }
          if (performerIds.length) input.performer_ids = Array.from(new Set(performerIds));
        }

        if (candidate.tagEntities && candidate.tagEntities.length) {
          const tagIds = (item.tagIds || []).slice();
          for (const tag of candidate.tagEntities) {
            const tagId = await resolveOrCreateTag(tag, candidate.sourceEndpoint);
            if (tagId) tagIds.push(tagId);
          }
          if (tagIds.length) input.tag_ids = Array.from(new Set(tagIds));
        }

        if (candidate.sourceEndpoint && candidate.remoteSiteId) {
          input.stash_ids = mergeStashIds(item.stashIds || [], candidate.sourceEndpoint, candidate.remoteSiteId);
        }

        await graphqlRequest(SCENE_UPDATE_MUTATION, { input });

        // Keep a compact local copy as an immediate rename fallback. The actual
        // source of truth is now the Scene metadata stored in Stash.
        const { image: _image, performerEntities: _performerEntities, tagEntities: _tagEntities, ...metadataCandidate } = candidate;
        const saved = {
          ...metadataCandidate,
          title: titleToSave || metadataCandidate.title || item.title,
          titleSource: useFilenameTitle ? "filename" : "graphql",
          originCandidateIdentity: candidateIdentity(candidate),
          savedAt: new Date().toISOString(),
          appliedToStash: true,
        };
        setSavedMetadata((previous) => {
          const next = { ...previous, [sceneId]: saved };
          safeWriteLocalStorage(SAVED_METADATA_STORAGE_KEY, next);
          return next;
        });

        // Immediately rename/move the file represented by this row using the
        // just-saved metadata. This removes the old two-step workflow where the
        // user had to save the Scene, tick the file, and run the batch renamer.
        const effectiveItem = applyMetadataOverride(item, saved);
        const autoPlan = buildRenamePlan(effectiveItem, stashPaths);
        let fileOperationOk = false;
        let fileOperationText = "";

        if (!autoPlan.ok) {
          fileOperationText = `Данные сохранены в Stash, но автоматическое переименование невозможно: ${autoPlan.error}`;
        } else if (!autoPlan.renameNeeded && !autoPlan.moveNeeded) {
          fileOperationOk = true;
          fileOperationText = "Файл уже имеет корректное имя и расположение.";
        } else {
          try {
            setOperationMessage({
              type: "info",
              text: `Scene ${sceneId} сохранена. Переименовывается/перемещается файл…`,
            });
            const moveData = await graphqlRequest(MOVE_FILE_MUTATION, {
              input: {
                ids: [autoPlan.fileId],
                destination_folder: autoPlan.destinationFolder,
                destination_basename: autoPlan.destinationBasename,
              },
            });
            if (!moveData || moveData.moveFiles !== true) {
              throw new Error("Stash вернул отрицательный результат moveFiles");
            }
            fileOperationOk = true;
            fileOperationText = `Файл переименован${autoPlan.moveNeeded ? " и перемещён" : ""}: ${autoPlan.destinationPath}`;
          } catch (moveErr) {
            console.error("[FilenameTitleChecker] Automatic rename after Scene save failed", item.path, moveErr);
            fileOperationText = `Данные сохранены в Stash, но файл не удалось переименовать/переместить: ${moveErr && moveErr.message ? moveErr.message : String(moveErr)}`;
          }
        }

        if (fileOperationOk) {
          // Keep the current review session intact. Remove only the processed
          // video row; other already-fetched GraphQL cards stay on screen.
          setIssues((previous) => previous.filter((issue) => issue.key !== item.key));
          setSelectedKeys((previous) => {
            const next = new Set(previous);
            next.delete(item.key);
            return next;
          });

          // GraphQL results are stored per Scene. Remove them only when there are
          // no other visible rows for this Scene, otherwise another file version
          // can still use the already-fetched candidates.
          const otherSceneRows = issues.some((issue) => issue.sceneId === sceneId && issue.key !== item.key);
          if (!otherSceneRows) {
            setLookupResults((previous) => {
              const next = { ...previous };
              delete next[sceneId];
              return next;
            });
          }

          const removedMismatchCount = item.reason === "mismatch" || item.reason === "no_title" ? 1 : 0;
          const removedNoTitle = item.reason === "no_title" ? 1 : 0;
          const removedNoFiles = item.reason === "no_files" ? 1 : 0;
          setStats((previous) => ({
            ...previous,
            mismatches: Math.max(0, previous.mismatches - removedMismatchCount),
            noTitle: Math.max(0, previous.noTitle - removedNoTitle),
            noFiles: Math.max(0, previous.noFiles - removedNoFiles),
          }));

          setOperationMessage({
            type: "success",
            text: `Данные ${candidate.sourceName || "GraphQL"} сохранены в Scene ${sceneId}${useFilenameTitle ? ` с Title «${titleToSave}» из имени файла` : ""}. ${fileOperationText} Обработанное видео скрыто. Для полной перепроверки нажмите «Проверить заново».`,
          });
        } else {
          // Keep the row visible so the user can inspect the saved metadata and
          // retry the file operation manually without losing GraphQL results.
          setOperationMessage({
            type: "warning",
            text: fileOperationText,
          });
        }
      } catch (err) {
        console.error("[FilenameTitleChecker] Failed to apply scraped metadata", err);
        setOperationMessage({ type: "danger", text: `Не удалось сохранить данные в Scene ${sceneId}: ${err && err.message ? err.message : String(err)}` });
      } finally {
        setMetadataSaving((previous) => { const next = new Set(previous); next.delete(sceneId); return next; });
      }
    }

    function clearSavedCandidate(sceneId) {
      setSavedMetadata((previous) => {
        const next = { ...previous };
        delete next[sceneId];
        safeWriteLocalStorage(SAVED_METADATA_STORAGE_KEY, next);
        return next;
      });
    }

    async function renameSelected() {
      if (renameState.running || !selectedPlans.length) return;

      const invalid = selectedPlans.filter(({ plan }) => !plan.ok);
      if (invalid.length) {
        setOperationMessage({
          type: "danger",
          text: `Невозможно обработать ${invalid.length} выбранных файлов. Снимите выбор с записей, для которых не удалось построить целевой путь.`,
        });
        return;
      }

      const changed = selectedPlans.filter(({ plan }) => plan.renameNeeded || plan.moveNeeded);
      if (!changed.length) {
        setOperationMessage({ type: "info", text: "Все выбранные файлы уже имеют рассчитанное имя и расположение." });
        return;
      }

      const movingCount = changed.filter(({ plan }) => plan.moveNeeded).length;
      const renamingCount = changed.filter(({ plan }) => plan.renameNeeded).length;
      const confirmed = window.confirm(
        `Будет обработано файлов: ${changed.length}.\n` +
        `Переименование требуется: ${renamingCount}.\n` +
        `Перемещение в папку текущей Studio требуется: ${movingCount}.\n\n` +
        `Шаблон: Date - Studio - Title - [WEBDL-Height].\n` +
        `Продолжить?`
      );
      if (!confirmed) return;

      setRenameState({ running: true, done: 0, total: changed.length });
      setOperationMessage(null);

      let success = 0;
      const failures = [];
      const successfulItems = [];

      for (let index = 0; index < changed.length; index += 1) {
        const { item, plan } = changed[index];
        try {
          const data = await graphqlRequest(MOVE_FILE_MUTATION, {
            input: {
              ids: [plan.fileId],
              destination_folder: plan.destinationFolder,
              destination_basename: plan.destinationBasename,
            },
          });

          if (!data || data.moveFiles !== true) {
            throw new Error("Stash вернул отрицательный результат moveFiles");
          }
          success += 1;
          successfulItems.push(item);
        } catch (err) {
          console.error("[FilenameTitleChecker] Rename/move failed", item.path, err);
          failures.push({
            path: item.path,
            message: err && err.message ? err.message : String(err),
          });
        }
        setRenameState({ running: true, done: index + 1, total: changed.length });
      }

      setRenameState({ running: false, done: changed.length, total: changed.length });

      // Keep the current review session intact. Successfully processed rows are
      // removed locally; failed rows stay visible (and selected) for retry.
      if (successfulItems.length) {
        const successfulKeys = new Set(successfulItems.map((item) => item.key));
        setIssues((previous) => previous.filter((item) => !successfulKeys.has(item.key)));
        setSelectedKeys((previous) => {
          const next = new Set(previous);
          successfulKeys.forEach((key) => next.delete(key));
          return next;
        });
        const removedMismatchCount = successfulItems.filter((item) => item.reason === "mismatch" || item.reason === "no_title").length;
        setStats((previous) => ({
          ...previous,
          mismatches: Math.max(0, previous.mismatches - removedMismatchCount),
        }));
      }

      if (failures.length) {
        const first = failures[0];
        setOperationMessage({
          type: "warning",
          text: `Успешно: ${success}. Ошибок: ${failures.length}. Успешные строки скрыты, строки с ошибками оставлены. Первая ошибка: ${first.path} — ${first.message}`,
        });
      } else {
        setOperationMessage({
          type: "success",
          text: `Готово: успешно переименовано/перемещено файлов: ${success}. Обработанные строки скрыты. Для полной перепроверки нажмите «Проверить заново».`,
        });
      }
    }

    function renderPagination() {
      if (pageCount <= 1) return null;
      return h(
        "div",
        { className: "ftc-pagination" },
        h(
          Button,
          {
            variant: "secondary",
            size: "sm",
            disabled: safePage <= 1 || renameState.running,
            onClick: function () { setPage(Math.max(1, safePage - 1)); },
          },
          "← Назад"
        ),
        h("span", { className: "ftc-page-label" }, "Страница"),
        h(
          "select",
          {
            className: "form-control form-control-sm ftc-page-select",
            value: String(safePage),
            disabled: renameState.running,
            title: "Перейти на страницу",
            onChange: function (event) { setPage(Number(event.target.value) || 1); },
          },
          Array.from({ length: pageCount }, function (_, index) {
            const pageNumber = index + 1;
            return h("option", { key: pageNumber, value: String(pageNumber) }, String(pageNumber));
          })
        ),
        h("span", { className: "ftc-page-total" }, `из ${pageCount}`),
        h(
          Button,
          {
            variant: "secondary",
            size: "sm",
            disabled: safePage >= pageCount || renameState.running,
            onClick: function () { setPage(Math.min(pageCount, safePage + 1)); },
          },
          "Вперёд →"
        )
      );
    }

    function renderIssueRow(item) {
      const sceneUrl = `/scenes/${item.sceneId}`;
      const preferred = savedMetadata[item.sceneId] || null;
      const effectiveItem = applyMetadataOverride(item, preferred);
      const plan = buildRenamePlan(effectiveItem, stashPaths);
      const selectable = plan.ok;
      const checked = selectedKeys.has(item.key);

      const sceneLookup = lookupResults[item.sceneId] || [];
      const sharedPerformers = sharedPerformerSignatures(sceneLookup);
      const isLookingUp = lookupLoading.has(item.sceneId);

      return h(
        React.Fragment,
        { key: item.key },
        h(
        "tr",
        { className: checked ? "ftc-row-selected" : "" },
        h(
          "td",
          { className: "ftc-select-cell" },
          h("input", {
            type: "checkbox",
            checked,
            disabled: !selectable || renameState.running,
            title: selectable ? "Выбрать файл" : plan.error,
            onChange: function () { toggleSelection(item); },
          })
        ),
        h(
          "td",
          { className: "ftc-preview-cell" },
          item.screenshot
            ? h(Link, { to: sceneUrl }, h("img", {
                className: "ftc-preview",
                src: item.screenshot,
                loading: "lazy",
                alt: item.title || item.filename || "Scene",
              }))
            : h("div", { className: "ftc-preview ftc-preview-empty" }, "—")
        ),
        h(
          "td",
          { className: "ftc-title-cell" },
          h(Link, { to: sceneUrl, className: "ftc-scene-title" }, item.title || "(без Title)"),
          item.studio ? h("div", { className: "ftc-studio" }, `Studio: ${item.studio}`) : h("div", { className: "ftc-studio" }, "Studio: No Studio"),
          item.filenameStudio
            ? h(
                "div",
                {
                  className: item.studioDiffersFilename ? "ftc-file-studio ftc-file-studio-different" : "ftc-file-studio",
                  title: "Студия, извлечённая из имени файла",
                },
                `В имени файла: ${item.filenameStudio}`
              )
            : null
        ),
        h(
          "td",
          { className: "ftc-file-cell" },
          h("div", { className: "ftc-filename", title: item.filename }, item.filename || "(нет файла)"),
          item.path ? h("div", { className: "ftc-path", title: item.path }, item.path) : null,
          plan.ok
            ? h(
                "div",
                { className: "ftc-target", title: plan.destinationPath },
                h("span", { className: "ftc-target-label" }, "→ "),
                plan.destinationPath
              )
            : item.fileId
              ? h("div", { className: "ftc-target ftc-target-error", title: plan.error }, `Нельзя переименовать: ${plan.error}`)
              : null
        ),
        h("td", null, h("span", { className: reasonClass(item.reason) }, reasonText(item.reason))),
        h(
          "td",
          { className: "ftc-action-cell" },
          h(Link, { to: sceneUrl, className: "btn btn-secondary btn-sm" }, "Открыть сцену"),
          h(
            Button,
            {
              variant: "info",
              size: "sm",
              className: "ftc-lookup-button",
              disabled: isLookingUp || lookupBatchState.running || renameState.running || stashBoxes.length === 0,
              onClick: function () { lookupScene(item); },
              title: stashBoxes.length ? "Запросить выбранные GraphQL-источники по fingerprints сцены" : "В Stash не настроены GraphQL/Stash-box источники",
            },
            isLookingUp ? "Запрос…" : "Сверить GraphQL"
          )
        )
        ),
        (sceneLookup.length || preferred || isLookingUp)
          ? h(
              "tr",
              { className: "ftc-graphql-detail-row" },
              h(
                "td",
                { colSpan: 6 },
                h(
                  "div",
                  { className: "ftc-graphql-panel" },
                  preferred
                    ? h(
                        "div",
                        { className: "ftc-saved-metadata" },
                        h("div", { className: "ftc-saved-title" }, `${preferred.appliedToStash ? "Сохранено в Stash" : "Сохранено для переименования"}: ${preferred.sourceName || "GraphQL"}`),
                        h("div", null, h("strong", null, preferred.titleSource === "filename" ? "Title (из имени файла): " : "Title: "), preferred.title || "—"),
                        h("div", null, h("strong", null, "Studio: "), preferred.studio || "—"),
                        h("div", null, h("strong", null, "Date: "), preferred.date || "—"),
                        preferred.performers && preferred.performers.length ? h("div", null, h("strong", null, "Performers: "), preferred.performers.join(", ")) : null,
                        h(Button, { variant: "outline-secondary", size: "sm", onClick: function () { clearSavedCandidate(item.sceneId); }, disabled: renameState.running || metadataSaving.has(item.sceneId), title: "Удаляет только локальный выбор; уже сохранённые метаданные Scene в Stash не откатываются" }, "Убрать локальный выбор")
                      )
                    : null,
                  isLookingUp ? h("div", { className: "ftc-graphql-loading" }, "Выполняются запросы к выбранным GraphQL-источникам…") : null,
                  sceneLookup.map(function (sourceResult) {
                    return h(
                      "div",
                      { className: "ftc-source-block", key: sourceResult.endpoint },
                      h(
                        "div",
                        { className: "ftc-source-heading" },
                        h("strong", null, sourceResult.name || sourceResult.endpoint),
                        h("span", { className: "ftc-source-endpoint", title: sourceResult.endpoint }, sourceResult.endpoint)
                      ),
                      sourceResult.status === "error"
                        ? h("div", { className: "alert alert-warning ftc-source-error" }, sourceResult.error || "Ошибка запроса")
                        : sourceResult.candidates.length === 0
                          ? h("div", { className: "ftc-source-empty" }, "Совпадений по fingerprints не найдено.")
                          : h(
                              "div",
                              { className: "ftc-candidate-grid" },
                              sourceResult.candidates.map(function (candidate, candidateIndex) {
                                const selected = Boolean(preferred) && (preferred.originCandidateIdentity ? preferred.originCandidateIdentity === candidateIdentity(candidate) : candidateIdentity(preferred) === candidateIdentity(candidate));
                                const isSavingMetadata = metadataSaving.has(item.sceneId);
                                const remoteUrl = remoteSceneUrl(candidate.sourceEndpoint, candidate.remoteSiteId);
                                const fileMetadata = extractMetadataFromFilename(item.filename);
                                const referenceTitle = fileMetadata.title || item.title;
                                const referenceStudio = fileMetadata.studio || item.studio;
                                const referenceDate = fileMetadata.date || item.date;
                                const titleMatchesFile = Boolean(candidate.title && referenceTitle) && normalizedText(candidate.title) === normalizedText(referenceTitle);
                                const studioMatchesFile = Boolean(candidate.studio && referenceStudio) && studioNamesEquivalent(candidate.studio, referenceStudio);
                                const dateMatchesFile = Boolean(candidate.date && referenceDate) && candidate.date === referenceDate;
                                const performersSignature = performerSetSignature(candidate.performers);
                                const performersMatchOtherSource = Boolean(performersSignature) && sharedPerformers.has(performersSignature);
                                return h(
                                  "div",
                                  { className: selected ? "ftc-candidate-card ftc-candidate-selected" : "ftc-candidate-card", key: `${sourceResult.endpoint}:${candidate.remoteSiteId || candidateIndex}` },
                                  h("div", { className: "ftc-candidate-number" }, sourceResult.candidates.length > 1 ? `Вариант ${candidateIndex + 1}` : "Результат"),
                                  candidate.image
                                    ? h(
                                        "div",
                                        { className: "ftc-candidate-image-wrap" },
                                        h("img", {
                                          className: "ftc-candidate-image",
                                          src: candidate.image,
                                          loading: "lazy",
                                          alt: candidate.title ? `Обложка: ${candidate.title}` : "Изображение результата GraphQL",
                                        })
                                      )
                                    : h("div", { className: "ftc-candidate-image-empty" }, "Источник не вернул изображение"),
                                  h("div", { className: titleMatchesFile ? "ftc-field-match" : "" }, h("strong", null, "Title: "), candidate.title || "—"),
                                  h("div", { className: studioMatchesFile ? "ftc-field-match" : "" }, h("strong", null, "Studio: "), candidate.studio || "—"),
                                  h("div", { className: dateMatchesFile ? "ftc-field-match" : "" }, h("strong", null, "Date: "), candidate.date || "—"),
                                  candidate.performers.length ? h("div", { className: performersMatchOtherSource ? "ftc-performers-consensus" : "" }, h("strong", null, "Performers: "), candidate.performers.join(", ")) : null,
                                  candidate.code ? h("div", null, h("strong", null, "Code: "), candidate.code) : null,
                                  candidate.director ? h("div", null, h("strong", null, "Director: "), candidate.director) : null,
                                  candidate.duration ? h("div", null, h("strong", null, "Duration: "), `${candidate.duration} s`) : null,
                                  candidate.tags.length ? h("div", { className: "ftc-candidate-tags" }, h("strong", null, "Tags: "), candidate.tags.join(", ")) : null,
                                  candidate.details ? h("details", { className: "ftc-candidate-details" }, h("summary", null, "Details"), h("div", null, candidate.details)) : null,
                                  candidate.urls && candidate.urls.length ? h("div", { className: "ftc-candidate-urls" }, h("strong", null, "URLs: "), candidate.urls.map(function (url, urlIndex) { return h("a", { href: url, target: "_blank", rel: "noreferrer", key: `${url}:${urlIndex}` }, urlIndex ? `Ссылка ${urlIndex + 1}` : "Ссылка"); })) : null,
                                  h(
                                    "div",
                                    { className: "ftc-candidate-actions" },
                                    h(Button, {
                                      variant: selected && preferred.titleSource !== "filename" ? "success" : "primary",
                                      size: "sm",
                                      disabled: renameState.running || isSavingMetadata,
                                      onClick: function () { saveCandidate(item.sceneId, candidate, item, { useFilenameTitle: false }); },
                                      title: "Сохранить выбранные метаданные в Scene Stash и сразу переименовать/переместить этот видеофайл",
                                    }, isSavingMetadata ? "Сохранение и переименование…" : (selected && preferred.titleSource !== "filename" ? "Сохранено в Stash" : "Сохранить в Scene и переименовать")),
                                    h(Button, {
                                      variant: selected && preferred.titleSource === "filename" ? "success" : "outline-primary",
                                      size: "sm",
                                      disabled: renameState.running || isSavingMetadata || !extractMetadataFromFilename(item.filename || basenameFromPath(item.path)).title,
                                      onClick: function () { saveCandidate(item.sceneId, candidate, item, { useFilenameTitle: true }); },
                                      title: extractMetadataFromFilename(item.filename || basenameFromPath(item.path)).title
                                        ? "Сохранить метаданные этого GraphQL-результата, но Title взять из текущего имени видеофайла; затем переименовать/переместить файл"
                                        : "Title не удалось определить из текущего имени файла",
                                    }, isSavingMetadata ? "Сохранение…" : (selected && preferred.titleSource === "filename" ? "Сохранено с именем файла" : "Сохранить в Scene с именем файла")),
                                    remoteUrl ? h("a", { href: remoteUrl, target: "_blank", rel: "noreferrer", className: "btn btn-secondary btn-sm" }, "Открыть источник") : null
                                  )
                                );
                              })
                            )
                    );
                  })
                )
              )
            )
          : null
      );
    }

    return h(
      "div",
      { className: "container-fluid ftc-page" },
      h(
        "div",
        { className: "ftc-header" },
        h("div", null,
          h("h2", null, "Filename Title Checker"),
          h("div", { className: "ftc-subtitle" }, "Проверяет Title, сравнивает данные нескольких GraphQL-источников и позволяет переименовать/переместить файлы по выбранным данным.")
        ),
        h(
          Button,
          {
            variant: "primary",
            onClick: scan,
            disabled: loading || renameState.running,
          },
          loading ? "Проверяется…" : "Проверить заново"
        )
      ),

      loading
        ? h(
            "div",
            { className: "ftc-progress-wrap" },
            h("div", { className: "ftc-progress-text" }, `Проверено сцен: ${progress.checked} из ${progress.total || "…"}`),
            h(
              "div",
              { className: "progress" },
              h("div", {
                className: "progress-bar",
                role: "progressbar",
                style: {
                  width: progress.total ? `${Math.min(100, (progress.checked / progress.total) * 100)}%` : "0%",
                },
              })
            )
          )
        : null,

      renameState.running
        ? h(
            "div",
            { className: "ftc-progress-wrap ftc-rename-progress" },
            h("div", { className: "ftc-progress-text" }, `Переименование/перемещение: ${renameState.done} из ${renameState.total}`),
            h(
              "div",
              { className: "progress" },
              h("div", {
                className: "progress-bar",
                role: "progressbar",
                style: {
                  width: renameState.total ? `${Math.min(100, (renameState.done / renameState.total) * 100)}%` : "0%",
                },
              })
            )
          )
        : null,

      error
        ? h("div", { className: "alert alert-danger ftc-error" }, `Ошибка проверки: ${error}`)
        : null,

      operationMessage
        ? h("div", { className: `alert alert-${operationMessage.type} ftc-operation-message` }, operationMessage.text)
        : null,

      h(
        "div",
        { className: "ftc-summary" },
        h(SummaryCard, { value: stats.scenes, label: "Сцен" }),
        h(SummaryCard, { value: stats.files, label: "Видеофайлов" }),
        h(SummaryCard, { value: stats.matches, label: "Совпадает" }),
        h(SummaryCard, { value: stats.mismatches, label: "Несовпадений" }),
        h(SummaryCard, { value: stats.noTitle, label: "Без Title" }),
        h(SummaryCard, { value: stats.noFiles, label: "Без файла" })
      ),

      h(
        "div",
        { className: "ftc-source-selector" },
        h(
          "div",
          { className: "ftc-source-selector-heading" },
          h("strong", null, "GraphQL-источники"),
          h("span", null, stashBoxes.length ? `Выбрано ${selectedSourceEndpoints.size} из ${stashBoxes.length}` : "Не настроены в Stash")
        ),
        stashBoxes.length
          ? h(
              "div",
              { className: "ftc-source-options" },
              stashBoxes.map(function (box) {
                return h(
                  "label",
                  { className: "ftc-source-option", key: box.endpoint, title: box.endpoint },
                  h("input", { type: "checkbox", checked: selectedSourceEndpoints.has(box.endpoint), disabled: lookupBatchState.running || renameState.running, onChange: function () { toggleSource(box.endpoint); } }),
                  h("span", null, box.name || box.endpoint)
                );
              })
            )
          : h("div", { className: "ftc-source-empty" }, "Добавьте GraphQL/Stash-box источники в Settings → Metadata Providers / Stash-boxes."),
        h("div", { className: "ftc-source-help" }, "Запрос выполняется через Stash по fingerprints сцены; API-ключи внешних источников не передаются в код плагина.")
      ),

      h(
        "div",
        { className: "ftc-filters" },
        h("input", {
          className: "form-control ftc-search",
          type: "search",
          placeholder: "Поиск по Title, имени файла, пути или студии…",
          value: search,
          disabled: renameState.running,
          onChange: function (event) { setSearch(event.target.value); },
        }),
        h(
          "select",
          {
            className: "form-control ftc-select",
            value: studio,
            disabled: renameState.running,
            onChange: function (event) { setStudio(event.target.value); },
          },
          h("option", { value: "all" }, "Все студии"),
          studios.map(function (name) { return h("option", { value: name, key: name }, name); })
        ),
        h(
          "select",
          {
            className: "form-control ftc-select",
            value: reason,
            disabled: renameState.running,
            onChange: function (event) { setReason(event.target.value); },
          },
          h("option", { value: "all" }, "Все проблемы"),
          h("option", { value: "mismatch" }, "Title не найден"),
          h("option", { value: "no_title" }, "Нет Title"),
          h("option", { value: "no_files" }, "Нет файла")
        ),
        h(
          "label",
          { className: "ftc-checkbox-option", title: "Показывать только несовпадения Title, где Studio сцены совпадает со Studio в имени файла" },
          h("input", {
            type: "checkbox",
            checked: sameStudioOnly,
            disabled: renameState.running,
            onChange: function (event) {
              const checked = event.target.checked;
              setSameStudioOnly(checked);
              if (checked) setDifferentStudioOnly(false);
            },
          }),
          h("span", null, "Только в пределах той же студии")
        ),
        h(
          "label",
          { className: "ftc-checkbox-option", title: "Показывать только несовпадения Title, где Studio в имени файла отличается от текущей Studio сцены" },
          h("input", {
            type: "checkbox",
            checked: differentStudioOnly,
            disabled: renameState.running,
            onChange: function (event) {
              const checked = event.target.checked;
              setDifferentStudioOnly(checked);
              if (checked) setSameStudioOnly(false);
            },
          }),
          h("span", null, "Только с разными студиями")
        ),
        h(
          "label",
          { className: "ftc-page-size-control" },
          h("span", null, "На странице:"),
          h(
            "select",
            {
              className: "form-control form-control-sm ftc-page-size-select",
              value: String(pageSize),
              disabled: renameState.running,
              title: "Количество видео на странице",
              onChange: function (event) {
                const nextSize = Number(event.target.value);
                if (!PAGE_SIZE_OPTIONS.includes(nextSize)) return;
                setPageSize(nextSize);
                setPage(1);
                safeWriteLocalStorage(PAGE_SIZE_STORAGE_KEY, nextSize);
              },
            },
            PAGE_SIZE_OPTIONS.map(function (size) {
              return h("option", { key: size, value: String(size) }, String(size));
            })
          )
        ),
        h("div", { className: "ftc-result-count" }, `Показано: ${filteredIssues.length}`)
      ),

      filteredIssues.length
        ? h(
            "div",
            { className: "ftc-batch-toolbar" },
            h("div", { className: "ftc-batch-count" },
              selectedItems.length === selectedPageItems.length
                ? `Выбрано: ${selectedPageItems.length}`
                : `Выбрано на странице: ${selectedPageItems.length} · всего: ${selectedItems.length}`
            ),
            h(
              Button,
              {
                variant: "primary",
                disabled: selectedItems.length === 0 || renameState.running || loading,
                onClick: renameSelected,
              },
              renameState.running ? "Обрабатывается…" : "Переименовать / переместить выбранные"
            ),
            h(
              Button,
              {
                variant: "info",
                disabled: selectedItems.length === 0 || renameState.running || lookupBatchState.running || stashBoxes.length === 0 || selectedSourceEndpoints.size === 0,
                onClick: lookupSelected,
              },
              lookupBatchState.running ? `GraphQL ${lookupBatchState.done}/${lookupBatchState.total}` : "Сверить GraphQL для выбранных"
            ),
            h(
              Button,
              {
                variant: "secondary",
                disabled: selectedItems.length === 0 || renameState.running,
                onClick: function () { setSelectedKeys(new Set()); },
              },
              "Снять выбор"
            ),
            h(
              "div",
              { className: "ftc-batch-help" },
              "Новое имя и путь показаны зелёной строкой под текущим файлом."
            )
          )
        : null,

      !loading && !error && filteredIssues.length === 0
        ? h(
            "div",
            { className: "alert alert-success ftc-empty" },
            issues.length === 0
              ? "Несовпадений не найдено."
              : "По текущим фильтрам несовпадений нет."
          )
        : null,

      filteredIssues.length
        ? h(
            "div",
            { className: "table-responsive ftc-table-wrap" },
            h(
              "table",
              { className: "table table-striped table-hover ftc-table" },
              h(
                "thead",
                null,
                h("tr", null,
                  h(
                    "th",
                    { className: "ftc-select-cell" },
                    h("input", {
                      type: "checkbox",
                      checked: allPageSelected,
                      disabled: eligiblePageItems.length === 0 || renameState.running,
                      title: allPageSelected ? "Снять выбор с доступных видео на этой странице" : "Выбрать все доступные видео на этой странице",
                      onChange: toggleAllOnPage,
                    })
                  ),
                  h("th", null, "Превью"),
                  h("th", null, "Title / Studio"),
                  h("th", null, "Файл → новое имя/путь"),
                  h("th", null, "Проблема"),
                  h("th", null, "")
                )
              ),
              h("tbody", null, pageItems.map(renderIssueRow))
            )
          )
        : null,

      renderPagination(),

      h(
        "div",
        { className: "ftc-help" },
        h("strong", null, "Переименование: "),
        h("code", null, "Date - Studio - Title - [WEBDL-Height].ext"),
        ". Расширение исходного видео сохраняется. Недопустимые символы имени файла заменяются на ",
        h("code", null, "-"),
        ".",
        h("br"),
        h("strong", null, "Перемещение: "),
        "файл переносится в папку текущей Studio внутри того Stash Library Path, где он находится. Если Studio была изменена, целевая папка автоматически меняется.",
        h("br"),
        h("strong", null, "GraphQL-сверка: "),
        "результаты запрашиваются по fingerprints сцены через настроенные в Stash Stash-box источники. Кнопка «Сохранить в Scene и переименовать» сохраняет выбранные метаданные непосредственно в карточку Scene Stash и сразу переименовывает/перемещает соответствующий видеофайл. Кнопка «Сохранить в Scene с именем файла» делает то же самое, но Title берёт из текущего имени видеофайла.",
        h("br"),
        h("strong", null, "Безопасность: "),
        "обрабатываются только выбранные видеофайлы. SRT/VTT/JPG/PNG автоматически не переносятся."
      )
    );
  }

  PluginApi.register.route(ROUTE, FilenameTitleCheckerPage);

  PluginApi.patch.before("MainNavBar.MenuItems", function (props) {
    return [
      {
        ...props,
        children: h(
          React.Fragment,
          null,
          props.children,
          h(
            NavLink,
            {
              to: ROUTE,
              className: "ftc-nav-link nav-link",
              activeClassName: "active",
              title: "Filename Title Checker",
            },
            "Filename Check"
          )
        ),
      },
    ];
  });

  window.FilenameTitleChecker = Object.freeze({
    normalizeTokens,
    containsTokenSequence,
    normalizedStudioKey,
    studioNamesEquivalent,
    extractStudioFromFilename,
    studioComparison,
    studioNameForFilename,
    replaceIllegalCharacters,
    buildTargetBasename,
    buildRenamePlan,
    applyMetadataOverride,
    compactScrapedCandidate,
  });
})();
